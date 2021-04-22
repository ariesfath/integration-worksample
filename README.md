# integration-worksample

## About
This project is for a proof of concept work sample for our integrations team.

## Instructions
### How to Run
1. Copy `data/job_config.json.example` into `data/job_config.json` and fill it with your Airtable base ID and token.
   You can configure how many tracking number will processed at a time by modifying the `batch_process_ct` value in the config file.
2. Install the python dependencies. It is recommended to run it within [virtual env](https://pypi.org/project/virtualenv/)
    ```
    # pip install -r requirements.py
    ```
3. Run the job
    ```
    # python run.py
    ```

### Access Setup
1. Follow the provided link to access the AirTable base. You may have to create an account to access the base.
2. Your base will have one table called Shipment Tracking.
3. A link will also be provided to access the AirTable API CRUD spec for this base.
4. Retrieve your API Key as instructed in the Authentication section of the CRUD spec

### Project Requirements
1. Fork this repo and create the code necessary for the follow requirements.
1. You will retrieve the Tracking Number for each of the records in the table and run them through the ShipIt API in 
   order to determine the carrier's pick up date & time for each tracking number.
    1. if a tracking number begins with "1Z" it is a UPS tracking number
    1. if a tracking number is a 12-digit numeric it is a FedEx tracking number
    1. if multiple tracking numbers exist for a single record, only use the first one to obtain the carrier pick up 
       date/time from ShipIt

1. You will use the ShipIt API vis-à-vis a heroku app URL to obtain the Carrier Pick-up Date & Time:
    1. http://shipit-api.herokuapp.com/api/carriers/fedex/trackingID
    1. http://shipit-api.herokuapp.com/api/carriers/ups/trackingID

1. You will parse the JSON response seeking the date/time associated with the Carrier Pick-up event.
    1. The event name from UPS is called the "Origin scan"
    1. The event name from FedEx is called "Picked up"

1. You will populate the “Carrier Pickup” property in the table with the date and time you retrieved for each record's 
   pick-up event. The date / time you will post in AirTable shall be in EST (UTC-5) format.

1. Reply to the original email with a link to your fork so we can review the code.
