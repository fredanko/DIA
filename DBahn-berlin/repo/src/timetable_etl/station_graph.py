from graph_tool import Graph
from .sql import load_sql

def build_station_graph(conn):
    g = Graph(directed=False)

    # vertex properties: station eva and name
    v_eva = g.new_vertex_property("long")
    v_name = g.new_vertex_property("string")

    # load stations
    with conn.cursor() as cur:
        cur.execute("""
            SELECT eva, name
            FROM stationen
            ORDER BY eva
        """)
        stations = cur.fetchall()

    eva_to_vertex = {}

    for eva, name in stations:
        v = g.add_vertex()
        v_eva[v]  = eva
        v_name[v] = name
        eva_to_vertex[eva] = v

    g.vertex_properties["eva"] = v_eva
    g.vertex_properties["name"] = v_name


    # load edges
    with conn.cursor() as cur:
        cur.execute(load_sql("graph/station_edges.sql"))
        edges = cur.fetchall()

    for eva_u, eva_v in edges:
        g.add_edge(eva_to_vertex[eva_u], eva_to_vertex[eva_v])

    return g, eva_to_vertex
