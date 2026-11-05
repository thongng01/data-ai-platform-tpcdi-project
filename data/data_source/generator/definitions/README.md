# Source Definitions and Personal Word Lists

`sources.yaml` and `finwire.yaml` define source field order, types, widths, and encoding. The CSV files supply small, fixed word lists for synthetic personal data. Generation does not download data or call a personal-data service.

| File | Contents | Source and reduction | Terms |
| --- | --- | --- | --- |
| `given_names.csv` | 128 male names and 128 female names with counts | Social Security Administration (SSA), [Top names of the 1960s](https://www.ssa.gov/oact/babynames/decades/names1960s.html). Copy ranks 1 through 128 for each sex, retaining the published counts. | US government work, public domain in the United States under 17 USC 105. The [federal catalog entry](https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-data) also identifies the underlying national dataset as CC0. |
| `surnames.csv` | 512 surnames with counts | Census Bureau, [2010 Census surname files](https://www.census.gov/topics/population/genealogy/data/2010_surnames.html), `Names_2010Census.csv` in the [download archive](https://www2.census.gov/topics/genealogy/2010surnames/names.zip). Keep the first 512 ranked rows, convert names to title case, and keep only `name` and `count`. | US government work, public domain in the United States under 17 USC 105. |
| `locations.csv` | 24 city, state, postal code, and area code combinations | Join the Census Bureau's [2010 ZCTA-to-place relationship file](https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_place_rel_10.txt) to the [2024 national places Gazetteer](https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_place_national.zip) on `GEOID`. For each committed postal code, select the place with the largest `POPPT`, copy `USPS` as the state, and remove the final ` city` from `NAME`. Each selected place contains more than 99% of its ZCTA's 2010 population. Area codes are manually supplied geographic facts. | Census fields are US government works, public domain in the United States under 17 USC 105. Area codes are facts, not copied explanatory text. |

The [federal open-license guidance](https://resources.data.gov/open-licenses/) explains the public-domain status of US government works. These lists contain aggregate frequencies and geographic facts, not source records about individual people. The generator copies no third-party name library.

## Selection and Contact Rules

The generator uses the existing stable random function with an entity identity, the configured seed, and a separate field purpose. It samples names in proportion to the retained counts. It uses the SSA male list for `M` and the female list for `F`. A separate name draw supplies the middle initial for 85% of people. The lists cover common adult names; they do not model each birth cohort or predict a person's gender from a name.

The generator selects a complete location row so that city, state, postal code, and area code agree. ZCTAs are Census approximations of postal delivery areas, not a postal address validation service. Locations have equal selection weights. Street words, house numbers, and apartment numbers are authored synthetic combinations, not copied household addresses. Street words appear in `personal_data.py`; no external terms apply to them.

Primary telephone numbers contain the selected area code and a local number from `555-0100` through `555-0199`. [NANPA reserves this range for fictional use](https://www.nanpa.com/numbering/555-line-numbers). Numbers can repeat because this safe range is small. Emails use `example.com` and `example.net`, which [IANA reserves for examples](https://www.iana.org/help/example-domains). The address contains a name initial, surname, and stable entity identity. No generated contact detail is intended for delivery.

Customers and matching prospects use the same personal identity and seed. They therefore share names, middle initial, gender, address, and primary telephone number. Unmatched prospects use identities above the run's customer range. Employee fields use a separate key namespace. Personal fields do not depend on customer tier, worker count, row order, or batch size.
