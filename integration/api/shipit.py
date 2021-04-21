import requests
import datetime


class ShippingProvider:
    UPS = 'ups'
    FEDEX = 'fedex'


class ShipItShipmentStatus:
    """Model class to parse ShipIt shipment status response"""
    def __init__(self, tracking_number: str, activities: list):
        self.tracking_number = tracking_number
        self.activities = activities

    def get_delivery_time(self) -> datetime.datetime:
        for activity in self.activities:
            if activity.get('details') == "Delivered":
                # Always deal with UTC to avoid headache
                date_str = activity.get("timestamp")
                date_str = date_str.replace("Z", "+00:00")
                return datetime.datetime.fromisoformat(date_str)

    @classmethod
    def parse_api_response(cls, api_response):
        try:
            tracking_number = api_response['request']['trackingNumber']
            activites = api_response['activities']
            return ShipItShipmentStatus(tracking_number, activites)
        except KeyError:
            raise Exception("Cannot parse API response")


class ShipIt:
    """Helper class to interact with ShipIt API
    """
    
    BASE_URL = "http://shipit-api.herokuapp.com/api/carriers"

    def _detect_provider(self, tracking_number: str):
        if tracking_number[:2] == '1Z':
            return ShippingProvider.UPS
        
        elif len(tracking_number) == 12 and tracking_number.isnumeric():
            return ShippingProvider.FEDEX

    def get_shipment_status(self, tracking_number: str) -> ShipItShipmentStatus:
        """Get shipment status based on a tracking number.
        Will try to detect shipping provider based on the tracking number format.
        Return ShipItShipmentStatus object, otherwise will raise Exception on failure.
        """
        provider = self._detect_provider(tracking_number)
        if not provider:
            raise Exception("Unknown tracking number format")

        url = "{base_url}/{provider}/{tracking_number}".format(
            base_url=self.BASE_URL,
            provider=provider,
            tracking_number=tracking_number
        )
        response = requests.get(url).json()
        return ShipItShipmentStatus.parse_api_response(response)
