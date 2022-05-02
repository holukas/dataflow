from pandas import DataFrame

try:
    # For CLI
    from ..db.dbclients import get_query_client
except:
    # For BOX
    from db.dbclients import get_query_client


class dbScanner:
    """
    <bucket>
        <measurement>
            <field1>
            <field2>
            ...
    """

    def __init__(self,
                 conf_db: dict):
        self.client, self.query_client = get_query_client(conf_db=conf_db)

        # bucketlist = self.show_buckets()
        # for bucket in bucketlist:
        #     measurementslist = self.show_measurements_in_bucket(bucket=bucket)
        #     print(f"Bucket: {bucket} | Measurements: {measurementslist}")
        #     varnameslist = self.show_varnames_in_bucket(bucket=bucket)
        #     for ix, v in enumerate(varnameslist):
        #         print(f"    {ix}: {v}")

        # bucket = 'ch-dav_raw'
        # measurements = ['TA', 'SW']
        # vars = ['TA_T1_35_1', 'TA_PRF_T1_35_1', 'SW_IN_T1_35_2', 'SW_OUT_T2_2.10_1']
        # start = '2022-01-27T00:00:00Z'
        # stop = 'now()'
        #
        # querystring = self._assemble_fluxql_querystring(bucket=bucket, measurements=measurements, vars=vars,
        #                                                 start=start,
        #                                                 stop=stop)
        # xxx = self.get_var_data(querystring=querystring)
        #
        # print(xxx.head(5))
        # print(xxx.tail(5))

        bucket = 'ch-dav_raw'
        measurement = 'PPFD'
        varnameslist = self.list_varnames_in_measurement(bucket=bucket, measurement=measurement)
        print(f"BUCKET: {bucket}  |  "
              f"MEASUREMENT: {measurement}  |  "
              f"{len(varnameslist)} VARIABLES (fields): {varnameslist}")

    def list_varnames_in_measurement(self, bucket: str, measurement: str) -> list:
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementFieldKeys(
        bucket: "{bucket}",
        measurement: "{measurement}")
        '''
        results = self.query_client.query_data_frame(query=query)
        varnameslist = results['_value'].tolist()
        return varnameslist

    def list_varnames_in_bucket(self, bucket: str) -> list:
        query = f'''
        import "influxdata/influxdb/schema"
        schema.fieldKeys(bucket: "{bucket}")
        '''
        results = self.query_client.query_data_frame(query=query)
        varnameslist = results['_value'].tolist()
        return varnameslist

    def list_measurements_in_bucket(self, bucket: str) -> list:
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket}")
        '''
        results = self.query_client.query_data_frame(query=query)
        measurementslist = results['_value'].tolist()
        return measurementslist

    def list_buckets(self) -> list:
        query = '''
        buckets()    
        '''
        # |> pivot(rowKey:["id"], columnKey: ["name"], valueColumn: "organizationID")
        results = self.query_client.query_data_frame(query=query)
        results.drop(columns=['result', 'table'], inplace=True)
        # results.set_index("name", inplace=True)
        bucketlist = results['name'].tolist()
        bucketlist = [x for x in bucketlist if not x.startswith('_')]
        return bucketlist

    def _fluxql_filterstring(self, queryfor: str, querylist: list) -> str:
        filterstring = ''  # Query string
        for ix, var in enumerate(querylist):
            if ix == 0:
                filterstring += f'|> filter(fn: (r) => r["{queryfor}"] == "{var}"'
            else:
                filterstring += f' or r["{queryfor}"] == "{var}"'
        filterstring = f"{filterstring})"  # Needs bracket at end
        return filterstring

    def _fluxql_bucketstring(self, bucket: str) -> str:
        return f'from(bucket: "{bucket}")'

    def _fluxql_rangestring(self, start: str, stop: str) -> str:
        return f'|> range(start: {start}, stop: {stop})'

    def _assemble_fluxql_querystring(self,
                                     bucket: str,
                                     start: str,
                                     stop: str,
                                     measurements: list,
                                     vars: list) -> str:
        _bucketstring = self._fluxql_bucketstring(bucket=bucket)
        _rangestring = self._fluxql_rangestring(start=start, stop=stop)
        _filterstring_m = self._fluxql_filterstring(queryfor='_measurement', querylist=measurements)
        _filterstring_v = self._fluxql_filterstring(queryfor='_field', querylist=vars)
        _keepstring = f'|> keep(columns: ["_time", "_field", "_value"])'
        _pivotstring = f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        querystring = f"{_bucketstring} {_rangestring} {_filterstring_m} {_filterstring_v} " \
                      f"{_keepstring} {_pivotstring}"
        # |> filter(fn: (r) => r.cpu == "cpu-total")  # tags?
        return querystring

    def get_var_data(self, querystring: str) -> DataFrame:
        results = self.query_client.query_data_frame(query=querystring)
        results.drop(columns=['result', 'table'], inplace=True)
        results.set_index("_time", inplace=True)
        return results
