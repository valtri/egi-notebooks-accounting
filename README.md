# EGI Notebooks Accounting

EGI Notebooks accounting tools.

# Usage

See *notebooks-accounting/values.yaml* for Helm package values.

Either grid certificate for APEL is required:

    ssm:
      hostcert: ...
      hostkey: ...

Or APEL needs to be disabled:

    ssm:
      # APEL sender not scheduled
      schedule:
    storage:
      # APEL dump files would be kept, this will disable APEL dumps
      apelSpool:

## Local database

By default local database export to */accounts/notebooks.db* is enabled. It can be disabled by setting location to empty value:

    storage:
      notebooksDb:

## FQAN configuration

FQAN filed mapping for accounting.

By default the values are taken from *primary\_group* field (=the first matched OIDC role in hub config *allowed\_groups*).

Example (multiple values per FQAN possible, separated by comma):

    accounting:
      # default_fqan: vo.notebooks.egi.eu
      # fqan_key: primary_group
      fqan:
        vo.access.egi.eu: urn:mace:egi.eu:group:vo.access.egi.eu:role=member#aai.egi.eu
        vo.notebooks.egi.eu: urn:mace:egi.eu:group:vo.notebooks.egi.eu:role=member#aai.egi.eu
