"""Frozen CAMS PV/wind contract, verified against the October 2026 raw samples.

45 hourly and seven 3-hourly parameters; never interpolate missing native leads.
Names/IDs follow the official ADS/ECMWF catalogue. Raw units and levels were
verified on all 14 forecast inits in docs/validation/cams-week-multisource-20261020.
"""
from dataclasses import dataclass

DATASET = "cams-global-atmospheric-composition-forecasts"
SOURCE = "ec_cams"
AREA = (55, 70, 15, 140)  # north, west, south, east; ADS native 0.4 degree grid
ENERGY = frozenset({169, 228129, 228021, 228022, 47, 176, 175, 177, 179})
PRECIP = frozenset({228, 143})


@dataclass(frozen=True)
class Variable:
    api_name: str
    param_id: int
    interval_hours: int
    pressure_level: int | None
    short_name: str
    units: str
    type_of_level: str
    level: int

    @property
    def leads(self) -> tuple[int, ...]:
        return tuple(range(0, 121, self.interval_hours))


VARIABLES = (
    Variable('total_aerosol_optical_depth_469nm', 210213, 1, None, 'aod469', '~', 'surface', 0),
    Variable('total_aerosol_optical_depth_550nm', 210207, 1, None, 'aod550', '~', 'surface', 0),
    Variable('total_aerosol_optical_depth_670nm', 210214, 1, None, 'aod670', '~', 'surface', 0),
    Variable('total_aerosol_optical_depth_865nm', 210215, 1, None, 'aod865', '~', 'surface', 0),
    Variable('total_aerosol_optical_depth_1240nm', 210216, 1, None, 'aod1240', '~', 'surface', 0),
    Variable('dust_aerosol_optical_depth_550nm', 210209, 1, None, 'duaod550', '~', 'surface', 0),
    Variable('sea_salt_aerosol_optical_depth_550nm', 210208, 1, None, 'ssaod550', '~', 'surface', 0),
    Variable('organic_matter_aerosol_optical_depth_550nm', 210210, 1, None, 'omaod550', '~', 'surface', 0),
    Variable('black_carbon_aerosol_optical_depth_550nm', 210211, 1, None, 'bcaod550', '~', 'surface', 0),
    Variable('sulphate_aerosol_optical_depth_550nm', 210212, 1, None, 'suaod550', '~', 'surface', 0),
    Variable('nitrate_aerosol_optical_depth_550nm', 210250, 1, None, 'niaod550', 'dimensionless', 'surface', 0),
    Variable('ammonium_aerosol_optical_depth_550nm', 210251, 1, None, 'amaod550', 'dimensionless', 'surface', 0),
    Variable('particulate_matter_2.5um', 210073, 1, None, 'pm2p5', 'kg m**-3', 'surface', 0),
    Variable('particulate_matter_10um', 210074, 1, None, 'pm10', 'kg m**-3', 'surface', 0),
    Variable('single_scattering_albedo_550nm', 215140, 1, None, 'ssa550', '(0 - 1)', 'surface', 0),
    Variable('asymmetry_factor_550nm', 215158, 1, None, 'asymmetry550', '~', 'surface', 0),
    Variable('total_absorption_aerosol_optical_depth_550nm', 215104, 1, None, 'aodabs550', '~', 'surface', 0),
    Variable('total_fine_mode_aerosol_optical_depth_550nm', 215122, 1, None, 'aodfm550', '~', 'surface', 0),
    Variable('10m_u_component_of_wind', 165, 1, None, '10u', 'm s**-1', 'surface', 0),
    Variable('10m_v_component_of_wind', 166, 1, None, '10v', 'm s**-1', 'surface', 0),
    Variable('2m_temperature', 167, 1, None, '2t', 'K', 'surface', 0),
    Variable('2m_dewpoint_temperature', 168, 1, None, '2d', 'K', 'surface', 0),
    Variable('skin_temperature', 235, 1, None, 'skt', 'K', 'surface', 0),
    Variable('surface_pressure', 134, 1, None, 'sp', 'Pa', 'surface', 0),
    Variable('mean_sea_level_pressure', 151, 1, None, 'msl', 'Pa', 'surface', 0),
    Variable('total_column_water_vapour', 137, 1, None, 'tcwv', 'kg m**-2', 'surface', 0),
    Variable('boundary_layer_height', 159, 1, None, 'blh', 'm', 'surface', 0),
    Variable('10m_wind_gust_in_the_last_3_hours', 228028, 3, None, '10fg3', 'm s**-1', 'surface', 0),
    Variable('most_unstable_convective_available_potential_energy', 228235, 1, None, 'mucape', 'J kg**-1', 'mostUnstableParcel', 0),
    Variable('surface_solar_radiation_downwards', 169, 1, None, 'ssrd', 'J m**-2', 'surface', 0),
    Variable('surface_solar_radiation_downward_clear_sky', 228129, 1, None, 'ssrdc', 'J m**-2', 'surface', 0),
    Variable('total_sky_direct_solar_radiation_at_surface', 228021, 1, None, 'fdir', 'J m**-2', 'surface', 0),
    Variable('clear_sky_direct_solar_radiation_at_surface', 228022, 1, None, 'cdir', 'J m**-2', 'surface', 0),
    Variable('direct_solar_radiation', 47, 1, None, 'dsrp', 'J m**-2', 'surface', 0),
    Variable('surface_net_solar_radiation', 176, 1, None, 'ssr', 'J m**-2', 'surface', 0),
    Variable('surface_thermal_radiation_downwards', 175, 1, None, 'strd', 'J m**-2', 'surface', 0),
    Variable('surface_net_thermal_radiation', 177, 1, None, 'str', 'J m**-2', 'surface', 0),
    Variable('top_net_thermal_radiation', 179, 1, None, 'ttr', 'J m**-2', 'surface', 0),
    Variable('forecast_albedo', 243, 1, None, 'fal', '(0 - 1)', 'surface', 0),
    Variable('total_cloud_cover', 164, 1, None, 'tcc', '(0 - 1)', 'surface', 0),
    Variable('low_cloud_cover', 186, 1, None, 'lcc', '(0 - 1)', 'surface', 0),
    Variable('medium_cloud_cover', 187, 1, None, 'mcc', '(0 - 1)', 'surface', 0),
    Variable('high_cloud_cover', 188, 1, None, 'hcc', '(0 - 1)', 'surface', 0),
    Variable('total_precipitation', 228, 1, None, 'tp', 'm', 'surface', 0),
    Variable('convective_precipitation', 143, 1, None, 'cp', 'm', 'surface', 0),
    Variable('snow_depth', 141, 1, None, 'sd', 'm of water equivalent', 'surface', 0),
    Variable('u_component_of_wind', 131, 3, 850, 'u', 'm s**-1', 'isobaricInhPa', 850),
    Variable('v_component_of_wind', 132, 3, 850, 'v', 'm s**-1', 'isobaricInhPa', 850),
    Variable('temperature', 130, 3, 850, 't', 'K', 'isobaricInhPa', 850),
    Variable('specific_humidity', 133, 3, 850, 'q', 'kg kg**-1', 'isobaricInhPa', 850),
    Variable('relative_humidity', 157, 3, 850, 'r', '%', 'isobaricInhPa', 850),
    Variable('geopotential', 129, 3, 850, 'z', 'm**2 s**-2', 'isobaricInhPa', 850),
)
BY_PARAM = {v.param_id: v for v in VARIABLES}
GROUPS = {
    "hourly": tuple(v for v in VARIABLES if v.interval_hours == 1),
    "gust": tuple(v for v in VARIABLES if v.param_id == 228028),
    "pressure850": tuple(v for v in VARIABLES if v.pressure_level == 850),
}
EXPECTED_MESSAGES = sum(len(v.leads) for v in VARIABLES)  # 5732


def request_for(date: str, cycle: int, group: str) -> dict:
    import datetime as dt
    day = dt.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    if cycle not in (0, 12):
        raise ValueError("CAMS cycles must be 00/12 UTC")
    variables = GROUPS[group]
    request = {
        "date": day, "time": [f"{cycle:02d}:00"], "type": ["forecast"],
        "variable": [v.api_name for v in variables],
        "leadtime_hour": [str(h) for h in variables[0].leads],
        "area": list(AREA), "data_format": "grib",
    }
    if group == "pressure850":
        request["pressure_level"] = ["850"]
    return request


def request_variables(request: dict, date: str, cycle: int, group: str) -> tuple[Variable, ...]:
    """Accept only an exact native subset; area/init/format/level cannot change."""
    original = request_for(date, cycle, group)
    axes = {'variable', 'leadtime_hour'}
    if set(request) != set(original) or any(request[k] != original[k] for k in set(original)-axes):
        raise ValueError('CAMS request differs from the fixed init/grid/format/level contract')
    for key in axes:
        values = request[key]
        if (not isinstance(values, list) or not values or any(not isinstance(v, str) for v in values)
                or len(values) != len(set(values)) or not set(values) <= set(original[key])):
            raise ValueError('CAMS request is not a unique native variable/lead subset')
    return tuple(v for v in GROUPS[group] if v.api_name in request['variable'])
