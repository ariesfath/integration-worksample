import airtable
import datetime
import pytz


AIRTABLE_DATE_FORMAT = "%m/%d/%Y"
AIRTABLE_DATETIME_FORMAT = "%m/%d/%Y %H:%M"


def to_airtable_datetime(date_obj: datetime.datetime) -> datetime.datetime:
    """Convert UTC datetime into EST datetime to be stored in Airtable"""
    est_tzinfo = pytz.timezone("EST")
    est_time = date_obj.astimezone(est_tzinfo)
    return est_time


class AirtablePurchaseOrder:
    """Model class to parse PO shipment data from Airtable"""

    SHIPMENT_FIELDS = ["PO", "Tracking Number", "Requested Ship Date"]

    def __init__(self, po_number: str, requested_ship_date: datetime.datetime=None,
                 diff_in_req_vs_shipped: datetime.timedelta=None):
        self.po_number = po_number
        self.requested_ship_date = requested_ship_date
        self.diff_in_req_vs_shipped = diff_in_req_vs_shipped

    def calculate_diff_kpi(self, delivery_time: datetime.datetime):
        self.diff_in_req_vs_shipped = delivery_time - self.requested_ship_date
        return self.diff_in_req_vs_shipped

    @classmethod
    def parse_api_response(cls, api_response):
        fields = api_response['fields']
        po_number = fields['PO']
        requested_ship_date_str = fields['Requested Ship Date']
        requested_ship_date = datetime.datetime.strptime(requested_ship_date_str,
                                                         AIRTABLE_DATETIME_FORMAT)
        est_tzinfo = pytz.timezone("EST")
        requested_ship_date = requested_ship_date.replace(tzinfo=est_tzinfo)
        return AirtablePurchaseOrder(po_number, requested_ship_date)


class AirtableRepository:
    """"Helper class to retrieve data from Airtable"""

    def __init__(self, app_id:str, token:str):
        self.shipments = airtable.Airtable(app_id, "Shipment Tracking", token)

    def get_unprocessed_shipments(self, last_update: datetime.datetime, max_records=10):
        """Get shipments that's not yet picked up by shipping provider"""
        is_processed_filter = "{Carrier Pickup}=''"
        result_iter = self.shipments.get_iter(max_records=max_records, 
                                              fields=AirtablePurchaseOrder.SHIPMENT_FIELDS,
                                              formula=is_processed_filter)
        return result_iter

    def update_carrier_pickup_time(self, po_number: str, pickup_time: datetime.datetime):
        est_time = to_airtable_datetime(pickup_time)
        update_data = {
            "Carrier Pickup": est_time.strftime(AIRTABLE_DATETIME_FORMAT)
        }
        result = self.shipments.update_by_field("PO", po_number, update_data)
        return result
