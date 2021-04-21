from integration.job.retrieve_shipping_kpi import (
    RetrieveShippingKpiJob, RetrieveShippingKpiJobConfig
)

config = RetrieveShippingKpiJobConfig.load_from_file('data/job_config.json')
job = RetrieveShippingKpiJob(config)
job.execute()
