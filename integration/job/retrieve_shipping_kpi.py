import datetime
import json
from ..api.airtable_repository import AirtableRepository, AirtablePurchaseOrder
from ..api.shipit import ShipIt
from ..logger import JobLogger


class RetrieveShippingKpiJobConfig:
    def __init__(self, airtable_base_id: str, airtable_token: str,
                 last_run_ts: int):
        self.airtable_base_id = airtable_base_id
        self.airtable_token = airtable_token
        self.last_run = datetime.datetime.fromtimestamp(last_run_ts)

    @classmethod
    def load_from_file(self, filepath):
        config = None
        with open(filepath, 'r') as config_file:
            config_data = json.load(config_file)
            config = RetrieveShippingKpiJobConfig(
                airtable_base_id=config_data.get('airtable_base_id'),
                airtable_token=config_data.get('airtable_token'),
                last_run_ts=config_data.get('last_run', 0)
            )
        return config

    @classmethod
    def write_to_file(cls, config, filepath):
        config_data = {
            "airtable_base_id": config.airtable_base_id,
            "airtable_token": config.airtable_token,
            "last_run": int(config.last_run.timestamp())
        }
        with open(filepath, 'w') as config_file:
            json.dump(config_data, config_file)

    def update_last_run(self, current_time=None):
        current_time = current_time or datetime.datetime.now()
        self.last_run = current_time


class RetrieveShippingKpiJob:
    def __init__(self, config: RetrieveShippingKpiJobConfig):
        self.config = config
        self.shipit = ShipIt()
        self.repository = AirtableRepository(config.airtable_base_id,
                                             config.airtable_token)

    def execute(self, current_time=None):
        current_time = current_time or datetime.datetime.now()
        last_run = self.config.last_run
        JobLogger.debug("Fetching unprocessed shipment data from Airtable since {last_run}...".format(
            last_run=last_run
        ))
        results = self.repository.get_unprocessed_shipments(last_run, 5)
        updated_po_numbers = list()
        for page in results:
            for record in page:
                po_number = record['fields']['PO']
                tracking_number = record['fields']['Tracking Number']
                try:
                    shipment_data = self.shipit.get_shipment_status(tracking_number)
                    pickup_time = shipment_data.get_carrier_pickup_time()
                except Exception as e:  # TODO: Use more specific exception
                    JobLogger.error(str(e))
                    continue

                if not pickup_time:
                    continue

                JobLogger.debug("PO {po_number} has been picked up on {pickup_time}".format(
                    po_number=po_number,
                    pickup_time=pickup_time
                ))

                self.repository.update_carrier_pickup_time(po_number, pickup_time)
                updated_po_numbers.append(po_number)
        JobLogger.debug("Successfully processed {po_ct} shipment status(es)".format(
            po_ct=len(updated_po_numbers)
        ))
        self.config.update_last_run(current_time)