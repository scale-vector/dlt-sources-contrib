
import requests
import time
from requests import Session
import json




class MetabaseStatsApi:
    _url: str = None
    _user: str = None
    _password: str = None

    def __init__(self, url: str, user: str, password: str) -> None:
        """

        Your description goes here

        :param url: Metabase api url
        :param user: Metabase username
        :param password: Metabase password

        :return: new MetabaseApi instance
        """
        self._url = url
        self._user = user
        self._password = password

    @property
    def url(self) -> str:
        return self._url.strip("/")

    @property
    def user(self) -> str:
        return self._user

    @property
    def password(self) -> str:
        return self._password

    @property
    def session(self) -> Session:
        payload = dict(username=self.user,
                       password=self.password)

        response = requests.post(f"{self.url}/api/session",
                                 data=json.dumps(payload),
                                 headers={"Content-Type": "application/json"})

        response.raise_for_status()

        json_body = response.json()

        json_body["X-Metabase-Session"] = json_body.pop("id")
        json_body["Content-Type"] = "application/json"

        session = requests.Session()

        session.headers.update(json_body)

        return session



    def _get_data(self, endpoint, params=None):
        print(f"{self.url}/api/{endpoint}")
        res = self.session.get(f"{self.url}/api/{endpoint}", params=params)
        request_time = time.time()
        res_json = res.json()
        #print(json.dumps(res_json,        indent = 4, sort_keys = True))

        # if list
        if isinstance(res_json, list):
            data = res_json
        else:
            if 'data' in res_json:
                data = res_json.get('data')
            else:
                data = [res_json]
        #print(data)
        print(type(data))

        #add metadata
        for d in data:
            d['endpoint'] = f"{self.url}/api/{endpoint}"
            d['request_time'] = request_time
            d['request_params'] = str(params)

        return data



    def _get_database_ids(self):
        databases = self._get_data('database')
        database_ids = [d['id'] for d in databases]
        return database_ids


    def _get_fields_endpoints(self):
        return [f"database/{id}/fields" for id in self._get_database_ids()]


    def get_rss_data(self):
        """ logs like reading cards or dashboards
        avtivity endpoint returns stuff like card edits
                :return:
        """
        endpoints = [dict(endpoint='activity', table='activity'),
                     dict(endpoint='util/logs', table='logs'),
                     ]

        for endpoint in endpoints:
            print(endpoint)
            data = self._get_data(endpoint.get('endpoint'), params=endpoint.get('params'))
            yield endpoint.get('table'), data


    def get_stateful_data(self):
        """
        get the stateful configured dashboard. Item creation times,
        :return:
        """
        endpoints = [#dict(endpoint='util/stats', table='stats'),
                     dict(endpoint='card', table='cards'),
                   #  dict(endpoint='user', table='users', params={'status': 'all'})
                    ]

        # get fields endpoints, does a call to get db ids.
        for p in self._get_fields_endpoints():
            endpoints.append(dict(endpoint=p, table='fields'))

        # load serially

        for endpoint in endpoints:
            print(endpoint)
            data = self._get_data(endpoint.get('endpoint'), params=endpoint.get('params'))
            yield endpoint.get('table'), data


if __name__ == "__main__":

    print('running')

    creds = dict(url='https://metabase-rasa-analytics.scalevector.ai/',
                 user='adrian+1@scalevector.ai',
                 password='***')

    m = MetabaseStatsApi(**creds)

    tables = dict()
    for table, data in m.get_rss_data():
        if not tables.get(table, None):
            tables[table] = []
        tables[table] += data
    print(tables)

    for table, data in m.get_stateful_data():
        if not tables.get(table, None):
            tables[table] = []
        tables[table] += data

    with open("tables.json", "w") as file:
        json.dump(tables, file, indent=4, sort_keys=True)

    import base64
    from dlt.pipeline import Pipeline, GCPPipelineCredentials

    for tablename, rows in tables.items():




        # 1. configuration: name your schema, table, pass credentials
        schema_prefix = 'metabase_'
        schema_name = 'api'
        parent_table = f'metabase_{tablename}'
        # gcp_credential_json_file_path = "/Users/adrian/PycharmProjects/sv/dlt/temp/scalevector-1235ac340b0b.json"
        gcp_credentials_json = {
            "type": "service_account",
            "project_id": "zinc-mantra-353207",
            "private_key": "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",
            # noqa
            "client_email": "data-load-tool-public-demo@zinc-mantra-353207.iam.gserviceaccount.com",
        }

        # we do not want to have this key verbatim in repo so we decode it here
        gcp_credentials_json["private_key"] = bytes([_a ^ _b for _a, _b in
                                                     zip(base64.b64decode(gcp_credentials_json["private_key"]),
                                                         b"quickstart-sv" * 150)]).decode("utf-8")

        # if you re-use an edited schema, then uncomment this part, so you can save it to file
        # schema_file_path = "examples/schemas/quickstart.yml"

        # 2. Create a pipeline
        credential = GCPPipelineCredentials.from_services_dict(gcp_credentials_json, schema_prefix)
        pipeline = Pipeline(schema_name)
        pipeline.create_pipeline(credential)

        # If you want to re-use a curated schema, uncomment the below
        # schema = Pipeline.load_schema_from_file(schema_file_path)
        # pipeline.create_pipeline(credential, schema=schema)

        # 3. Pass the data to the pipeline and give it a table name. Optionally unpack and handle schema.


        pipeline.extract(iter(rows), table_name=parent_table)
        # tell the pipeline to un-nest the json into a relational structure
        pipeline.unpack()

        # If you want to save the schema to curate it and re-use it, uncomment the below
        # schema = pipeline.get_default_schema()
        # schema_yaml = schema.as_yaml()
        # f = open(data_schema_file_path, "a")
        # f.write(schema_yaml)
        # f.close()

        # 4. Load
        pipeline.load()

        # 5. Optional error handling - print, raise or handle.

        # now enumerate all complete loads if we have any failed packages
        # complete but failed job will not raise any exceptions
        completed_loads = pipeline.list_completed_loads()
        # print(completed_loads)
        for load_id in completed_loads:
            print(f"Checking failed jobs in {load_id}")
            for job, failed_message in pipeline.list_failed_jobs(load_id):
                print(f"JOB: {job}\nMSG: {failed_message}")