import airtable
import datetime


AIRTABLE_DATE_FORMAT = "%m/%d/%Y"
AIRTABLE_DATETIME_FORMAT = "%m/%d/%Y %H:%M"

class AirtablePurchaseOrder:
    """Model class to parse PO shipment data from Airtable"""
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
        # TODO: Assumed that ShipIt date is UTC, need to confirm later
        requested_ship_date = requested_ship_date.replace(tzinfo=datetime.timezone.utc)
        return AirtablePurchaseOrder(po_number, requested_ship_date)


class AirtableRepository:
    """"Helper class to retrieve data from Airtable"""

    SHIPMENT_FIELDS = ["PO", "Tracking Number", "Requested Ship Date", "Diff in Req'd vs Ship Date"]

    def __init__(self, app_id:str, token:str):
        self.shipments = airtable.Airtable(app_id, "Shipment Tracking", token)

    def get_unprocessed_shipments(self, last_update: datetime.datetime, max_records=10):
        """Get shipments that's not yet have processed kpis"""
        date_filter = "IS_AFTER({{Record Last Modified DateTime}}, DATETIME_PARSE('{date_str}'))".format(
            date_str=last_update.strftime(AIRTABLE_DATE_FORMAT)
        )
        is_processed_filter = "{Carrier Pickup}=''"
        formula = "AND({date_filter}, {is_processed_filter})".format(
            date_filter=date_filter,
            is_processed_filter=is_processed_filter
        )
        result_iter = self.shipments.get_iter(max_records=max_records, 
                                              fields=self.SHIPMENT_FIELDS,
                                              formula=formula)
        return result_iter

    def update_shipment_kpi(self, po_number: str, diff_kpi: datetime.timedelta):
        """Update Diff in Req'd vs Ship Date in Airtable shipment"""
        update_data = {
            "Diff in Req'd vs Ship Date": str(diff_kpi)
        }
        result = self.shipments.update_by_field("PO", po_number, update_data)
        return result

    def update_carrier_pickup_time(self, po_number: str, pickup_time: datetime.datetime):
        update_data = {
            "Carrier Pickup": pickup_time.strftime(AIRTABLE_DATETIME_FORMAT)
        }
        result = self.shipments.update_by_field("PO", po_number, update_data)
        return result
