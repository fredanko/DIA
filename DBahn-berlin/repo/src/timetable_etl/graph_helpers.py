# different methods for visualising / informing about / debugging graphs

def print_station_edges(g, eva, eva_to_vertex):
    v = eva_to_vertex[eva]
    name = g.vertex_properties["name"]

    print(f"Connections of {name[v]} ({eva}):")
    for u in v.all_neighbors():
        print("  ->", name[u])
