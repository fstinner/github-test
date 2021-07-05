import pymortar
import os

client = pymortar.Client(endpoint="https://beta-api.mortardata.org/")
query = """SELECT ?meter WHERE {?meter rdf:type/rdfs:subClassOf* brick:Electric_Meter};"""
resp = client.qualify([query])
if resp.error != "":
    print("ERROR: ", resp.error)
    os.exit(1)
print("running on {0} sites".format(len(resp.sites)))
print(resp.sites)

meter_view = pymortar.View(
    name="meters",
    sites=resp.sites,
    definition=query,
)
print(meter_view)

streams = pymortar.DataFrame(
    name="data",
    aggregation=pymortar.MEAN,
    window="15m",
    timeseries=[
        pymortar.Timeseries(
            view=meter_view,
            dataVars=["?meter"],
        )
    ],
)
print(streams)
time_params = pymortar.TimeParams(
    start="2016-01-01T00:00:00Z",
    end="2016-02-01T00:00:00Z",
)
request = pymortar.FetchRequest(
    sites=resp.sites,  # from our call to Qualify
    views=[meter_view],
    dataFrames=[streams],
    time=time_params,
)
result = client.fetch(request)

