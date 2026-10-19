"""History counts must not confuse object inventory with usable probabilities."""
import importlib.util
from pathlib import Path

import httpx
import pytest


def module():
    spec=importlib.util.spec_from_file_location('history_audit',Path(__file__).parents[1]/'scripts/audit_probability_history.py')
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    return m


def test_inventory_follows_empty_pages_before_counting_missing():
    m=module();calls=[]
    name=next(iter(m.object_names('20261015','ifs',0)))
    def respond(request):
        calls.append(request.url.params.get('pageToken'))
        return httpx.Response(200,json={'nextPageToken':'next'} if len(calls)==1 else {'items':[{'name':name,'size':'100'}]})
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result=m.list_day(client,'20261015')
    assert calls==[None,'next']
    assert result['objects'][0]['name']==name


def test_missing_one_of_four_objects_is_not_a_complete_init():
    m=module();names=sorted(m.object_names('20261015','ifs',0))
    rows=m.init_rows([{'date':'20261015','objects':[{'name':n,'size':'100'} for n in names[:3]]}])
    row=next(r for r in rows if r['model']=='ifs' and r['cycle']==0)
    assert not row['objects_complete'] and row['objects_found']==3
    assert row['missing_objects']==names[3:]


def test_duplicate_index_record_is_rejected():
    m=module()
    records=[dict(type='ep',param=p,step=w) for p,ws in m.expected_windows('ifs',240).items() for w in ws]
    assert m.check_index(records,'ifs',240)[0]['complete']
    records.append(records[0])
    assert not m.check_index(records,'ifs',240)[0]['complete']


def test_header_length_mismatch_is_rejected_and_correct_header_passes():
    m=module()
    header=b'GRIB'+b'\x00\x00\x00\x02'+(100).to_bytes(8,'big')
    with httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(206,content=header))) as c:
        assert m.sample_header(c,'https://example.test/data',{'_offset':0,'_length':100})['valid']
        assert not m.sample_header(c,'https://example.test/data',{'_offset':0,'_length':99})['valid']


def test_ignored_range_never_consumes_full_bundle():
    m=module()
    class MustNotRead(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError('must not consume a full forecast bundle')
    with httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(200,stream=MustNotRead()))) as client:
        result=m.sample_header(client,'https://example.test/data',{'_offset':0,'_length':100})
    assert result=={'valid':False,'status':200}


def test_ranges_do_not_bridge_missing_cycles():
    m=module()
    got=m.intervals(['2026-10-13T00:00:00+00:00','2026-10-13T12:00:00+00:00','2026-10-14T12:00:00+00:00'])
    assert len(got)==2
    assert got[0][1]=='2026-10-13T12:00:00+00:00'


def test_summary_keeps_inventory_counts_separate_from_verified_counts():
    m=module();names=m.object_names('20261015','ifs',0)
    rows=m.init_rows([dict(date='20261015',objects=[dict(name=n,size='100') for n in names])])
    summary=m.summarize(rows,[dict(model='ifs',init_utc='2026-10-15T00:00:00+00:00',status='index_or_header_mismatch')])['ifs']
    assert summary['pipeline_object_complete_inits']==1
    assert summary['statuses']=={'index_or_header_mismatch':1}
    assert summary['index_header_pass_intervals_12h']==[]


def test_legacy_offset_reset_can_be_rebased_when_lengths_cover_the_object(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'scripts'))
    from audit_probability_legacy_offsets import corrected_records
    records=[{'_offset':0,'_length':20},{'_offset':20,'_length':30},{'_offset':0,'_length':40}]
    assert [r['_offset'] for r in corrected_records(records,90)]==[0,20,50]
    assert records[2]['_offset']==0, 'analysis must not mutate the original index'


def test_legacy_rebase_rejects_a_partial_index(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'scripts'))
    from audit_probability_legacy_offsets import corrected_records
    with pytest.raises(ValueError,match='whole object'):
        corrected_records([{'_offset':0,'_length':20}],21)
