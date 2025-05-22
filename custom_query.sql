-- =============================================================================
-- Query for Looker Studio Custom Data Source
-- CALLS the pre-existing UDF: <PROJECT_ID>.profiler_data.buildAggregatedFlameGraphJson
-- *** CORRECTED OPTIONAL DATE FILTERING using PARSE_TIMESTAMP ***
-- =============================================================================
WITH
  -- 1: Filter profiles based on parameters and CORRECTED OPTIONAL DATE RANGE
  FilteredProfiles AS (
    SELECT
      t.profile_uuid, -- Include a unique ID for random selection step
      t.sample,
      t.location,
      t.function,
      t.sample_type,
      t.profile_type,
      t.deployment_target
    FROM
      -- <<< Replace with your actual project.dataset.table >>>
      `dogwood-harmony-449407-q2.profiler_data.profiler_denormalized_data` AS t
    WHERE
      -- Use the parameters defined in Looker Studio:
      t.profile_type = @profile_type
      AND t.deployment_target = @deployment_target
      AND ARRAY_LENGTH(t.sample) > 0 -- Ensure profiles have actual samples

      -- Assumes t.upload_timestamp is a TIMESTAMP or DATETIME column
      AND t.upload_timestamp >= COALESCE(
            SAFE.PARSE_TIMESTAMP('%Y%m%d', @DS_START_DATE),
            TIMESTAMP('0001-01-01 00:00:00+00')
          )
      AND t.upload_timestamp < COALESCE(
            -- Parse the END date and add 1 day to create an exclusive upper bound
            TIMESTAMP_ADD(SAFE.PARSE_TIMESTAMP('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY),
            TIMESTAMP('9999-12-31 23:59:59+00') -- Use a very late timestamp as the default upper bound
          )

      -- Add any other necessary non-date filters here
  ),

  -- 2: Select up to 250 profiles randomly from the filtered set
  SelectedProfiles AS (
    SELECT
      fp.sample,
      fp.location,
      fp.function,
      fp.sample_type,
      fp.profile_type,
      fp.deployment_target
    FROM FilteredProfiles fp
    ORDER BY RAND() -- Order randomly
    LIMIT 250 -- Take at most 250
  )

-- 3: Aggregate the *selected* profiles using the PRE-EXISTING UDF
SELECT
  -- *** Call the UDF using its full path ***
  `dogwood-harmony-449407-q2.profiler_data.buildAggregatedFlameGraphJson`(
    -- Aggregate profile data arrays into a single array for the UDF
    ARRAY_AGG(
      STRUCT(sp.sample, sp.location, sp.function, sp.sample_type)
    ),
    -- Pass the *actual count* of selected profiles (will be <= 250)
    COUNT(1)
  ) AS aggregatedFlameGraphJson
FROM
  SelectedProfiles sp
GROUP BY
  -- Group by the original filter criteria to ensure one aggregation result per parameter combination
  sp.profile_type,
  sp.deployment_target
