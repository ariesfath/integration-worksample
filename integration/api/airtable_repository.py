import airtable
import datetime


class AirtableRepository:
    """"Helper class to retrieve data from Airtable"""
    AIRTABLE_DATE_FORMAT = "%m/%d/%Y"

    SHIPMENT_FIELDS = ["PO", "Tracking Number", "Requested Ship Date", "Diff in Req'd vs Ship Date"]

    def __init__(self, app_id:str, token:str):
        self.shipments = airtable.Airtable(app_id, "Shipment Tracking", token)

    def get_unprocessed_shipments(self, last_update: datetime.datetime, max_records=10):
        """Get shipments that's not yet have processed kpis"""
        date_filter = "IS_AFTER({{Record Last Modified DateTime}}, DATETIME_PARSE('{date_str}'))".format(
            date_str=last_update.strftime(self.AIRTABLE_DATE_FORMAT)
        )
        is_processed_filter = "{Diff in Req'd vs Ship Date}=''"
        formula = "AND({date_filter}, {is_processed_filter})".format(
            date_filter=date_filter,
            is_processed_filter=is_processed_filter
        )
        result_iter = self.shipments.get_iter(max_records=max_records, 
                                              fields=self.SHIPMENT_FIELDS,
                                              formula=formula)
        return result_iter
