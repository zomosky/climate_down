"""Range/object checks and crash-safe concatenation guard original ADS bytes."""
import hashlib
import json
import pytest

requests=pytest.importorskip('requests')

from climate_download.cams import ranges
from climate_download.cams.state import save


@pytest.fixture
def server(monkeypatch):
    class Server:
        data=b'GRIBabcdefghijklmnopqrstuvwxyz7777'
        etag='"same-object"'
        calls=[]
        wrong=None
        fail_once=False
    server=Server()
    class Reply:
        status_code=206
        def __init__(self,start,end):
            self.start,self.end=start,end
            self.headers={'Content-Range':f'bytes {start}-{end}/{len(server.data)}',
                          'Content-Length':str(end-start+1),'ETag':server.etag}
            if end>0 and server.wrong=='range':self.headers['Content-Range']='bytes 0-1/2'
            if end>0 and server.wrong=='etag':self.headers['ETag']='"foreign-object"'
            if end>0 and server.wrong=='length':self.headers['Content-Length']='1'
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def close(self):pass
        def raise_for_status(self):pass
        def iter_content(self,size):
            data=server.data[self.start:self.end+1]
            if self.start==0 and self.end==7 and server.fail_once:
                server.fail_once=False
                yield data[:2]
                raise requests.ConnectionError('https://signed.invalid/private?token=private')
            if self.end>0 and server.wrong=='excess':data+=b'excess'
            for i in range(0,len(data),size):yield data[i:i+size]
    class Session:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def get(self,url,*,headers,stream,timeout):
            assert self.trust_env is False and stream
            assert headers['Accept-Encoding']=='identity'
            start,end=map(int,headers['Range'].split('=')[1].split('-'))
            server.calls.append((start,end))
            return Reply(start,end)
    monkeypatch.setattr(requests,'Session',Session)
    return server


def download(server,part,**kwargs):
    return ranges.transfer_ranges('https://signed.invalid/?token=private',part,len(server.data),
        request_id='original-job',connections=4,seconds=30,chunk_bytes=8,**kwargs)


def test_ranges_preserve_existing_prefix_and_append_each_byte_once(server,tmp_path):
    part=tmp_path/'download.part';part.write_bytes(server.data[:5])
    receipt,done=download(server,part)
    assert done and part.read_bytes()==server.data
    assert receipt['bytes_before']==5 and receipt['partial_bytes']==len(server.data)
    assert sum(end-start+1 for start,end in server.calls[1:])==len(server.data)-5
    assert not list(part.with_name(part.name+'.ranges').glob('*.part'))
    assert 'https://' not in json.dumps(receipt)


def test_connection_loss_resumes_same_cached_range_without_resetting_raw(server,tmp_path):
    part=tmp_path/'download.part';server.fail_once=True
    first,done=download(server,part)
    assert not done and first['transport_errors']=={'ConnectionError':1}
    before=part.read_bytes() if part.exists() else b''
    second,done=download(server,part)
    assert done and part.read_bytes()==server.data and part.read_bytes().startswith(before)
    assert (2,7) in server.calls


@pytest.mark.parametrize('wrong',['range','etag','length','excess'])
def test_unexpected_server_range_never_overwrites_existing_bytes(server,tmp_path,wrong):
    part=tmp_path/'download.part';part.write_bytes(server.data[:5]);server.wrong=wrong
    with pytest.raises(ranges.RangeIdentityError):download(server,part)
    assert part.read_bytes()==server.data[:5]


@pytest.mark.parametrize('change',['etag','request','length'])
def test_pinned_object_change_keeps_original_cache_and_partial(server,tmp_path,change):
    part=tmp_path/'download.part';server.fail_once=True;download(server,part)
    before=part.read_bytes() if part.exists() else b''
    if change=='etag':server.etag='"changed"'
    elif change=='length':server.data+=b'x'
    else:
        path=part.with_name(part.name+'.ranges')/'identity.json';identity=json.loads(path.read_text());identity['request_id']='foreign-job';save(path,identity)
    with pytest.raises(ranges.RangeIdentityError):download(server,part)
    assert (part.read_bytes() if part.exists() else b'')==before


def test_interrupted_append_resumes_at_verified_overlap_without_duplicate_bytes(server,tmp_path):
    part=tmp_path/'download.part';part.write_bytes(server.data[:3]);root=part.with_name(part.name+'.ranges');root.mkdir()
    save(root/'identity.json',{'request_id':'original-job','expected_bytes':len(server.data),'etag':server.etag,
        'base_offset':0,'chunk_bytes':8})
    chunk=root/'0-7.part';chunk.write_bytes(server.data[:8])
    save(root/'0-7.json',{'start':0,'end':7,'bytes':8,'sha256':hashlib.sha256(chunk.read_bytes()).hexdigest()})
    proof=json.loads((root/'0-7.json').read_text());save(root/'0-7.json',{**proof,'request_id':'original-job','etag':server.etag})
    _,done=download(server,part)
    assert done and part.read_bytes()==server.data and (0,7) not in server.calls


def test_corrupt_overlap_is_retained_and_blocks_commit(server,tmp_path):
    part=tmp_path/'download.part';part.write_bytes(b'BAD');root=part.with_name(part.name+'.ranges');root.mkdir()
    save(root/'identity.json',{'request_id':'original-job','expected_bytes':len(server.data),'etag':server.etag,
        'base_offset':0,'chunk_bytes':8})
    chunk=root/'0-7.part';chunk.write_bytes(server.data[:8])
    save(root/'0-7.json',{'start':0,'end':7,'bytes':8,'sha256':hashlib.sha256(chunk.read_bytes()).hexdigest()})
    proof=json.loads((root/'0-7.json').read_text());save(root/'0-7.json',{**proof,'request_id':'original-job','etag':server.etag})
    with pytest.raises(ranges.RangeIdentityError):download(server,part)
    assert part.read_bytes()==b'BAD' and chunk.exists()


@pytest.mark.parametrize('damage',['sha256','request_id','etag'])
def test_completed_cache_receipt_tampering_never_changes_the_original_prefix(server,tmp_path,damage):
    part=tmp_path/'download.part';server.fail_once=True;download(server,part)
    before=part.read_bytes() if part.exists() else b''
    root=part.with_name(part.name+'.ranges');path=root/'8-15.json'
    proof=json.loads(path.read_text());proof[damage]='foreign';save(path,proof)
    with pytest.raises(ranges.RangeIdentityError):download(server,part)
    assert (part.read_bytes() if part.exists() else b'')==before
