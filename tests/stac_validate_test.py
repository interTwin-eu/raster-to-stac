from openeo.local import LocalConnection

local_conn = LocalConnection("./")

cube = local_conn.load_stac("./to-validate.json")

cube.execute()