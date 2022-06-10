# tap-gorgias

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Gorgias](https://developers.gorgias.com/reference)
- Extracts the following resources:
  - [Tickets](https://developers.gorgias.com/reference#get_api-tickets)
    - Note that in order to pull tickets in descending order (as this tap requires) a new View object will need to be created. This is best done through the API with the following request:
    ```
    from tap_gorgias import GorgiasAPI

    config = {
        "subdomain": "<customer subdomain>",
        "username": "<username>",
        "password": "<password>",
        "start_date": "2021-03-01T00:00:00.000000Z",
    }

    client = GorgiasAPI(config)
    url = "https://{config['subdomain']}.gorgias.com/api/views"

    payload = {
        "category": "ticket-list",
        "order_by": "updated_datetime",
        "order_dir": "desc",
        "shared_with_users": [<your admin user id>],
        "type": "ticket-list",
        "visibility": "private",
        "slug": "pathlight-tickets"
    }

    resp = client.get(url, params=payload)
    id = resp['id']
    ```
    Then add this ID to the config as `tickets_view_id` and you're all set!
  - [Ticket Messages](https://developers.gorgias.com/reference#ticket-messages)
    - Note that this is a substream of tickets
  - [Satisfaction Surveys](https://developers.gorgias.com/reference#satisfaction-surveys)
  - [Events](https://developers.gorgias.com/reference/get_api-events)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2021 Pathlight
