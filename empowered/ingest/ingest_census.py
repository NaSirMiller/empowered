import asyncio
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional, Tuple


from empowered.utils.helpers import get_sql_client
from empowered.utils.logger_setup import set_logger, get_logger
from empowered.repositories.census.datasets_repo import DatasetRepository
from empowered.repositories.census.estimates_repo import CensusEstimateRepository
from empowered.repositories.census.geography_repo import GeographyRepository
from empowered.repositories.census.groups_repo import GroupsRepository
from empowered.repositories.census.variables_repo import VariablesRepository
from empowered.repositories.census.years_available_repo import YearsAvailableRepository
from empowered.repositories.census.checkpoint_repository import CheckpointRepository
from empowered.services.census_service import (
    get_counties,
    get_estimates,
    get_groups,
    get_places,
    get_states,
    get_variables,
)

# ---- CONFIG ----
# ACS API allows up to ~50 variables per request
VARS_PER_REQUEST = 50

NETWORK_CONCURRENCY = 40  # concurrent Census API calls
GROUPS_CONCURRENCY = 8  # concurrent group variable loads
GEOGRAPHY_CONCURRENCY = 20  # concurrent state-level geo fetchers
DB_CONCURRENCY = 10  # concurrent DB writer workers

# Thread pools
NETWORK_EXECUTOR = ThreadPoolExecutor(max_workers=NETWORK_CONCURRENCY)
DB_EXECUTOR = ThreadPoolExecutor(max_workers=DB_CONCURRENCY)

# Backoff / retry settings
MAX_RETRIES = 4
INITIAL_BACKOFF = 0.5  # seconds

ACS_DATASETS = [
    {"code": "acs", "frequency": 5, "id": "acs5"},
]
ACS_VARIABLES = [
    # Population & Households
    "B01003_001E",
    "B25001_001E",
    "B25002_001E",
    "B25010_001E",
    # Income
    "B19013_001E",
    "B19301_001E",
    "B19001_001E",
    # B19001 – Household Income Distribution
    "B19001_001E",
    "B19001_002E",
    "B19001_003E",
    "B19001_004E",
    "B19001_005E",
    "B19001_006E",
    "B19001_007E",
    "B19001_008E",
    "B19001_009E",
    "B19001_010E",
    "B19001_011E",
    "B19001_012E",
    "B19001_013E",
    "B19001_014E",
    "B19001_015E",
    "B19001_016E",
    "B19001_017E",
    # Housing Costs
    "B25064_001E",
    "B25077_001E",
    "B25070_001E",
    # Education
    "B15003_017E",
    "B15003_022E",
    "B15003_023E",
    "B15003_025E",
    # B14007 – School Enrollment
    "B14007_001E",
    "B14007_002E",
    "B14007_003E",
    "B14007_004E",
    "B14007_005E",
    "B14007_006E",
    "B14007_007E",
    "B14007_008E",
    "B14007_009E",
    "B14007_010E",
    "B14007_011E",
    "B14007_012E",
    "B14007_013E",
    "B14007_014E",
    "B14007_015E",
    "B14007_016E",
    "B14007_017E",
    "B14007_018E",
    "B14007_019E",
    # B14006 – Poverty Status by School Enrollment
    "B14006_001E",
    "B14006_002E",
    "B14006_003E",
    "B14006_004E",
    "B14006_005E",
    "B14006_006E",
    "B14006_007E",
    "B14006_008E",
    "B14006_009E",
    "B14006_010E",
    "B14006_011E",
    "B14006_012E",
    "B14006_013E",
    "B14006_014E",
    "B14006_015E",
    "B14006_016E",
    "B14006_017E",
    "B14006_018E",
    "B14006_019E",
    "B14006_020E",
    "B14006_021E",
    # B09010 – Children & Public Assistance
    "B09010_001E",
    "B09010_002E",
    "B09010_003E",
    "B09010_004E",
    "B09010_005E",
    "B09010_006E",
    "B09010_007E",
    "B09010_008E",
    "B09010_009E",
    "B09010_010E",
    "B09010_011E",
    "B09010_012E",
    "B09010_013E",
    # B17001 – Poverty Status by Sex and Age
    "B17001_001E",
    "B17001_002E",
    "B17001_003E",
    "B17001_004E",
    "B17001_005E",
    "B17001_006E",
    "B17001_007E",
    "B17001_008E",
    "B17001_009E",
    "B17001_010E",
    "B17001_011E",
    "B17001_012E",
    "B17001_013E",
    "B17001_014E",
    "B17001_015E",
    "B17001_016E",
    "B17001_017E",
    "B17001_018E",
    "B17001_019E",
    "B17001_020E",
    "B17001_021E",
    "B17001_022E",
    "B17001_023E",
    "B17001_024E",
    "B17001_025E",
    "B17001_026E",
    "B17001_027E",
    "B17001_028E",
]

ACS_GROUPS = list(set([var.split("_")[0] for var in ACS_VARIABLES]))
print(len(ACS_GROUPS))

YEARS = [2019, 2024]

set_logger()
logger = get_logger(__name__)


# -------------------------
# HELPERS
# -------------------------
def chunk_list(xs: List, size: int) -> Iterable[List]:
    for i in range(0, len(xs), size):
        yield xs[i : i + size]


async def arun(func: Callable, executor: ThreadPoolExecutor, *args, **kwargs):
    """Run a synchronous function in an executor and return its result."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


async def retry_async_call(
    func: Callable,
    executor: ThreadPoolExecutor,
    *args,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    **kwargs,
):
    """
    Wrapper: call `func` in executor with retries and exponential backoff.
    """
    attempt = 0
    while True:
        try:
            return await arun(func, executor, *args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.exception(
                    f"Max retries reached for {func.__name__} with args={args} kwargs={kwargs}"
                )
                raise
            backoff = (
                initial_backoff * (2 ** (attempt - 1)) * (1 + random.random() * 0.1)
            )
            logger.warning(
                f"Transient error calling {func.__name__} (attempt {attempt}/{max_retries}): {e}. Backing off {backoff:.2f}s"
            )
            await asyncio.sleep(backoff)


# -------------------------
# HIGH-LEVEL INGEST WORKFLOW
# -------------------------
async def load_groups_and_variables(
    dataset: dict,
    year: int,
) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    """
    Loads groups once and variables for each group once.
    Returns dict: {group_id: [variable_ids...]} (note: full variable dicts kept if desired)
    """
    acs_id = dataset["frequency"]
    dataset_id = dataset["id"]
    logger.info(
        f"[LOAD] Loading groups for dataset={dataset_id} year={year} (ACS{acs_id})"
    )
    groups_resp = await retry_async_call(
        get_groups, NETWORK_EXECUTOR, acs_id=acs_id, year=year
    )
    if not isinstance(groups_resp, list):
        # support both list or dict depending on your API wrapper
        groups = groups_resp.get("groups", groups_resp)
    else:
        groups = groups_resp

    group_ids = [g.get("group_id") or g.get("name") for g in groups]
    logger.info(f"[LOAD] Found {len(group_ids)} groups.")
    if ACS_GROUPS:
        group_ids = ACS_GROUPS.copy()
        groups = [g for g in groups if g.get("group_id") in ACS_GROUPS]
        logger.info(f"[FILTER] Will use {len(groups)} groups based on ACS_GROUPS.")

    # Load variables for groups concurrently but limited
    sem = asyncio.Semaphore(GROUPS_CONCURRENCY)
    variables_by_group: Dict[str, List[str]] = {}

    async def load_group_vars(gid: str):
        async with sem:
            logger.debug(f"[LOAD] Loading variables for group {gid} ...")
            vars_resp = await retry_async_call(
                get_variables, NETWORK_EXECUTOR, acs_id, year, gid
            )
            if isinstance(vars_resp, dict):
                vs = vars_resp.get("variables") or vars_resp
            else:
                vs = vars_resp
            variables_by_group[gid] = vs
            logger.info(f"[LOAD] Group {gid}: {len(vs)} variables loaded.")

    await asyncio.gather(*(load_group_vars(gid) for gid in group_ids))

    logger.info("[LOAD] All groups and variables loaded.")
    return variables_by_group, groups


async def ingest_groups_and_variables_to_db(
    dataset: dict,
    year: int,
    groups: List[Dict],
    variables_by_group: Dict[str, List[Dict]],
    groups_repo: GroupsRepository,
    variables_repo: VariablesRepository,
    year_repo: YearsAvailableRepository,
):
    """
    Insert groups and variables as batches into DB. Uses DB_EXECUTOR.
    """
    dataset_id = dataset["id"]
    year_id = year_repo.get_years(dataset_id=dataset_id, year=year)[0]["id"]

    logger.info(
        f"[DB] Inserting {len(groups)} groups into DB for dataset={dataset_id} year_id={year_id}"
    )
    variable_records = []
    for gid, variables in variables_by_group.items():
        for variable in variables:
            variable_records.append(
                {
                    **variable,
                    "group_id": gid,
                }
            )
    logger.info(
        f"[DB] Inserting {len(variable_records)} variables into DB (dataset={dataset_id} year_id={year_id})"
    )
    # prefer a batch insert in repo
    await arun(
        groups_repo.insert_groups,
        DB_EXECUTOR,
        groups=groups,
        dataset_id=dataset_id,
        year_id=year_id,
    )
    await arun(
        variables_repo.insert_variables,
        DB_EXECUTOR,
        variables=variable_records,
        dataset_id=dataset_id,
        year_id=year_id,
    )
    logger.info("[DB] Groups and variables inserted.")


async def load_geography_all(dataset: dict, year: int) -> List[Dict]:
    """
    Load states, and for each state, counties and places concurrently (bounded).
    Returns list of state dicts:
    [
      {"state_fips": "...", "state_name": "...", "counties": [...], "places": [...]},
      ...
    ]
    """
    acs_id = dataset["frequency"]
    logger.info(f"[LOAD] Loading states for ACS{acs_id} year={year}")
    states = await retry_async_call(get_states, NETWORK_EXECUTOR, acs_id, year)

    sem = asyncio.Semaphore(GEOGRAPHY_CONCURRENCY)

    async def fetch_state(state):
        state_fips = state["state_fips"]
        state_name = state.get("state_name")
        async with sem:
            logger.debug(
                f"[LOAD] Fetching counties+places for state {state_name} ({state_fips})"
            )
            counties_task = retry_async_call(
                get_counties, NETWORK_EXECUTOR, acs_id, year, state_fips
            )
            places_task = retry_async_call(
                get_places, NETWORK_EXECUTOR, acs_id, year, state_fips
            )
            counties, places = await asyncio.gather(counties_task, places_task)
            logger.info(
                f"[LOAD] State {state_name} ({state_fips}): {len(counties)} counties, {len(places)} places"
            )
            return {
                "state_fips": state_fips,
                "state_name": state_name,
                "counties": counties,
                "places": places,
            }

    states_list = (
        states
        if isinstance(states, list)
        else list(states.values()) if isinstance(states, dict) else states
    )
    results = await asyncio.gather(*(fetch_state(s) for s in states_list))
    logger.info("[LOAD] Completed geography load for all states.")
    return results


async def ingest_geography_to_db(
    dataset: dict,
    year: int,
    geography: List[Dict],
    geo_repo: GeographyRepository,
    year_repo: YearsAvailableRepository,
):
    """
    Batch insert states, counties, places. Uses DB executor.
    """
    dataset_id = dataset["id"]
    year_id = year_repo.get_years(dataset_id=dataset_id, year=year)[0]["id"]

    # Batch states
    states_batch = [
        {"state_fips": s["state_fips"], "state_name": s["state_name"]}
        for s in geography
    ]
    logger.info(
        f"[DB] Inserting {len(states_batch)} states for dataset={dataset_id} year_id={year_id}"
    )
    await arun(
        geo_repo.insert_states,
        DB_EXECUTOR,
        states=states_batch,
        dataset_id=dataset_id,
        year_id=year_id,
    )

    # Batch counties and places: flatten lists into batches for DB
    counties_flat = []
    places_flat = []

    for s in geography:
        for c in s["counties"]:
            c_record = {**c, "state_fips": s["state_fips"]}
            counties_flat.append(c_record)
        for p in s["places"]:
            p_record = {**p, "state_fips": s["state_fips"]}
            places_flat.append(p_record)

    logger.info(
        f"[DB] Inserting {len(counties_flat)} counties and {len(places_flat)} places"
    )
    # chunk DB writes to avoid extremely large single insert if lists huge
    CHUNK = 1000
    for chunk in chunk_list(counties_flat, CHUNK):
        await arun(
            geo_repo.insert_counties,
            DB_EXECUTOR,
            counties=chunk,
            dataset_id=dataset_id,
            year_id=year_id,
        )
    for chunk in chunk_list(places_flat, CHUNK):
        await arun(
            geo_repo.insert_places,
            DB_EXECUTOR,
            places=chunk,
            dataset_id=dataset_id,
            year_id=year_id,
        )

    logger.info("[DB] Geography inserted.")


# -----------------------
# ESTIMATE INGESTION (streaming + batching)
# -----------------------
async def estimate_worker(
    dataset: dict,
    year: int,
    variable_batch: List[str],
    place_fips: Optional[str],
    county_fips: Optional[str],
    state_fips: Optional[str],
    estimates_repo: CensusEstimateRepository,
    year_repo: YearsAvailableRepository,
    semaphore: asyncio.Semaphore,
):
    """
    Fetch estimate for a given geography and variable batch, then insert into DB.
    This function is safe to call many times concurrently; concurrency is controlled by `semaphore`.
    """
    async with semaphore:
        try:
            estimates_resp = await retry_async_call(
                get_estimates,
                NETWORK_EXECUTOR,
                dataset["frequency"],
                year,
                variable_batch,
                state_fips=state_fips,
                place_fips=place_fips,
                county_fips=county_fips,
            )
            estimates = estimates_resp.get("estimates", estimates_resp)
            estimates = [
                {
                    **e,
                    "place_fips": place_fips,
                    "state_fips": state_fips,
                    "county_fips": county_fips,
                }
                for e in estimates
            ]
            dataset_id = dataset["id"]
            year_id = year_repo.get_years(dataset_id=dataset_id, year=year)[0]["id"]
            await arun(
                estimates_repo.insert_estimates,
                DB_EXECUTOR,
                estimates=estimates,
                dataset_id=dataset_id,
                year_id=year_id,
            )
            logger.debug(
                f"[EST] Inserted {len(estimates)} estimates for place={place_fips} county={county_fips} state={state_fips} var_count={len(variable_batch)}"
            )
        except Exception as e:
            logger.exception(
                f"[EST][ERROR] Failed estimate for place={place_fips} county={county_fips} state={state_fips} vars={len(variable_batch)}: {e}"
            )


async def stream_and_run_estimates(
    dataset: dict,
    year: int,
    variables_by_group: Dict[str, List[str]],
    geography: List[Dict],
    estimates_repo: CensusEstimateRepository,
    year_repo: YearsAvailableRepository,
    place_only: bool = True,
):
    """
    Stream jobs (place or county) and run them with limited concurrency.
    Each job is a (variable_batch, place_fips, county_fips, state_fips) triple.
    """
    # prepare all variable batches across all groups (flat list of lists)
    all_variable_batches: List[List[str]] = []
    for gid, variables in variables_by_group.items():
        var_ids = [v["variable_id"] for v in variables]
        for batch in chunk_list(var_ids, VARS_PER_REQUEST):
            all_variable_batches.append(list(batch))

    logger.info(
        f"[EST] Prepared {len(all_variable_batches)} variable batches across groups (VARS_PER_REQUEST={VARS_PER_REQUEST})"
    )

    # prepare semaphore for network calls
    sem = asyncio.Semaphore(NETWORK_CONCURRENCY)

    # streaming: create worker tasks in limited windows to avoid out of memory error
    # create batches of jobs and await them;
    window_size = NETWORK_CONCURRENCY * 2  # at most double concurrent
    pending_tasks: List[asyncio.Task] = []

    async def schedule_job(variable_batch, place_fips, county_fips, state_fips):
        return await estimate_worker(
            dataset=dataset,
            year=year,
            variable_batch=variable_batch,
            place_fips=place_fips,
            county_fips=county_fips,
            state_fips=state_fips,
            estimates_repo=estimates_repo,
            year_repo=year_repo,
            semaphore=sem,
        )

    def enqueue_and_maybe_wait(task_coro):
        t = asyncio.create_task(task_coro)
        pending_tasks.append(t)
        # throttle: if pending grows, wait for some to finish
        if len(pending_tasks) >= window_size:
            return asyncio.gather(*pending_tasks)
        return None

    # iterate geography and schedule jobs
    submitted = 0
    start = time.perf_counter()
    for s in geography:
        state_fips = s["state_fips"]
        # counties
        if not place_only:
            for c in s["counties"]:
                county_fips = c["county_fips"]
                for variable_batch in all_variable_batches:
                    coro = schedule_job(
                        variable_batch=variable_batch,
                        place_fips=None,
                        county_fips=county_fips,
                        state_fips=state_fips,
                    )
                    maybe = enqueue_and_maybe_wait(coro)
                    submitted += 1
                    if maybe is not None:
                        # wait for window to finish
                        await maybe
                        pending_tasks.clear()
        # places
        for p in s["places"]:
            place_fips = p["place_fips"]
            for variable_batch in all_variable_batches:
                coro = schedule_job(
                    variable_batch=variable_batch,
                    place_fips=place_fips,
                    county_fips=None,
                    state_fips=state_fips,
                )
                maybe = enqueue_and_maybe_wait(coro)
                submitted += 1
                if maybe is not None:
                    await maybe
                    pending_tasks.clear()

    # wait for any leftover tasks
    if pending_tasks:
        await asyncio.gather(*pending_tasks)

    elapsed = time.perf_counter() - start
    logger.info(
        f"[EST] Submitted and completed {submitted} estimate jobs in {elapsed:.2f}s"
    )


async def run_ingest(
    dataset: dict,
    year: int,
    *,
    groups_repo: GroupsRepository,
    variables_repo: VariablesRepository,
    geo_repo: GeographyRepository,
    estimates_repo: CensusEstimateRepository,
    year_repo: YearsAvailableRepository,
    checkpoint_repo: CheckpointRepository,
    place_only: bool = True,
):
    dataset_id = dataset["id"]
    logger.info(f"=== START INGEST dataset={dataset_id} year={year} ===")
    t0 = time.perf_counter()

    # Load or create checkpoint
    checkpoint = checkpoint_repo.get_or_create(dataset_id, year)
    logger.info(f"Checkpoint state=\n{checkpoint}")

    # 1) Groups & Variables
    if not checkpoint["groups_ingested"] or not checkpoint["variables_ingested"]:
        variables_by_group, groups = await load_groups_and_variables(dataset, year)

        # Insert groups & variables to DB (batch)
        await ingest_groups_and_variables_to_db(
            dataset,
            year,
            groups,
            variables_by_group,
            groups_repo,
            variables_repo,
            year_repo,
        )

        # Update checkpoint flags
        checkpoint_repo.mark_completed(dataset_id, year, "groups_ingested")
        checkpoint_repo.mark_completed(dataset_id, year, "variables_ingested")
    else:
        logger.info("→ Skipping groups & variables ingestion (already completed)")

    # 2) Geography
    if not checkpoint["geography_ingested"]:
        geography = await load_geography_all(dataset, year)
        await ingest_geography_to_db(dataset, year, geography, geo_repo, year_repo)
        checkpoint_repo.mark_completed(dataset_id, year, "geography_ingested")
    else:
        logger.info("→ Skipping geography ingestion (already completed)")

    # 3) Estimates
    if not checkpoint["estimates_ingested"]:
        # Make sure variables_by_group and geography are loaded if previous stage skipped
        if "variables_by_group" not in locals() or "geography" not in locals():
            variables_by_group, _ = await load_groups_and_variables(dataset, year)
            geography = await load_geography_all(dataset, year)

        await stream_and_run_estimates(
            dataset,
            year,
            variables_by_group,
            geography,
            estimates_repo,
            year_repo,
            place_only=place_only,
        )
        checkpoint_repo.mark_completed(dataset_id, year, "estimates_ingested")
    else:
        logger.info("→ Skipping estimates ingestion (already completed)")

    total = time.perf_counter() - t0
    logger.info(
        f"=== COMPLETE INGEST dataset={dataset_id} year={year} (took {total:.2f}s) ==="
    )


# -----------------------
# main entrypoint
# -----------------------
async def main():
    start_total = time.perf_counter()

    client = get_sql_client()
    dataset_repo = DatasetRepository(client)
    groups_repo = GroupsRepository(client)
    variables_repo = VariablesRepository(client)
    geo_repo = GeographyRepository(client)
    estimates_repo = CensusEstimateRepository(client)
    year_repo = YearsAvailableRepository(client)
    checkpoint_repo = CheckpointRepository(client)

    for dataset in ACS_DATASETS:
        logger.info(f"===== START dataset {dataset['id']} =====")
        try:
            await arun(
                dataset_repo.insert_code,
                DB_EXECUTOR,
                dataset["code"],
                dataset["frequency"],
            )
        except:
            pass

        for year in YEARS:
            logger.info(f"--- START year {year} ---")
            try:
                await arun(
                    year_repo.insert_year,
                    DB_EXECUTOR,
                    dataset_id=dataset["id"],
                    year=year,
                )
            except:
                pass

            await run_ingest(
                dataset=dataset,
                year=year,
                groups_repo=groups_repo,
                variables_repo=variables_repo,
                geo_repo=geo_repo,
                estimates_repo=estimates_repo,
                year_repo=year_repo,
                checkpoint_repo=checkpoint_repo,
                place_only=True,
            )
            logger.info(f"--- FINISH year {year} ---")

    total_elapsed = time.perf_counter() - start_total
    logger.info(f"[TIMER] Total ingestion runtime: {total_elapsed:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
