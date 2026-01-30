# a graph is build following the time-expanded concept:
# nodes exist for all stations and all trips connecting 2 stations a and b
# edges connect station a with trip t and trip t with station b, with
# edge a -> t carrying the departure date of trip t from station a
# edge t -> b carrying the arrival date of trip t at station b
# transfer times are discarded: transfers are possible when arrival time ≤ departure time 

# compare to this stanford lecture script by john miller: https://theory.stanford.edu/~virgi/cs367/oldlecs/lecture6.pdf
# and the lecture: "Algorithmen für Routenplanung" by Dorothea Wagner, https://i11www.iti.kit.edu/_media/teaching/sommer2019/routenplanung/chap3-timetables.pdf

from graph_tool import Graph
from .sql import load_sql
from datetime import timezone
import heapq
from math import inf

def build_time_exp_graph(conn):
    g = Graph(directed=True)

    # vertex properties
    v_type = g.new_vertex_property("string")   # "station"or "trip"
    v_eva  = g.new_vertex_property("long")
    v_name = g.new_vertex_property("string")
    v_cat  = g.new_vertex_property("string")
    v_no   = g.new_vertex_property("string")
    v_line = g.new_vertex_property("string")

    # edge property: time weight (epoch seconds)
    e_time = g.new_edge_property("long")

    # stations 
    eva_to_v = {}

    with conn.cursor() as cur:
        cur.execute("SELECT eva, name FROM stationen")
        for eva, name in cur.fetchall():
            v = g.add_vertex()
            v_type[v] = "station"
            v_eva[v]  = eva
            v_name[v] = name
            eva_to_v[eva] = v

    # trips 
    with conn.cursor() as cur:
        cur.execute(load_sql("graph/trips.sql"))
        rows = cur.fetchall()

    for train_id, cat, number, line, eva_from, eva_to, dep_ts, arr_ts in rows:

        dep_sec = int(dep_ts.timestamp())
        arr_sec = int(arr_ts.timestamp())

        trip_v = g.add_vertex()
        v_type[trip_v] = "trip"
        v_cat[trip_v]  = cat
        v_no[trip_v]   = number
        v_line[trip_v] = line

        # station -> trip (departure)
        e1 = g.add_edge(eva_to_v[eva_from], trip_v)
        e_time[e1] = dep_sec

        # trip -> station (arrival)
        e2 = g.add_edge(trip_v, eva_to_v[eva_to])
        e_time[e2] = arr_sec

    g.vertex_properties.update(
        type=v_type, eva=v_eva, name=v_name, category=v_cat, train_no=v_no, line=v_line
    )
    g.edge_properties["time"] = e_time

    return g, eva_to_v


# earliest arrival = search for earliest incoming node at goal connected to start by a path (~modified Dijkstra)
def earliest_arrival(g, start, goal, dep_ts):
    e_time = g.edge_properties["time"]
    v_type = g.vertex_properties["type"]

    earliest = {v: inf for v in g.vertices()} # all nodes are infinitely far (~late) away
    parent   = {}
    #visited_trips = set() # track trips
 
    earliest[start] = dep_ts
    pq = [(dep_ts, start)]

    while pq:
        cur_time, u = heapq.heappop(pq)

        if cur_time > earliest[u]:
            continue
        if u == goal:
            break

        for e in u.out_edges():
            v = e.target()
            if v_type[u] == "station" and v_type[v] == "trip":
                dep = e_time[e]
                if dep < earliest[u]:
                    continue
                new_time = dep
            elif v_type[u] == "trip" and v_type[v] == "station":
                new_time = e_time[e]
            else:
                continue
            
            if new_time <= earliest[v]:
                earliest[v] = new_time
                parent[v] = (u,e)
                heapq.heappush(pq, (new_time, v))

# --- 
#     while pq:
#         cur_time, u = heapq.heappop(pq)

#         # skip node if already found better option
#         if cur_time > earliest[u]:
#             continue
#         if u == goal:
#             break

#         for e in u.out_edges():
#             v = e.target()
#             edge_time = e_time[e] # dep or arr time

# # ------- version with 6 day delay
#             # station -> trip: only take trip if dep time (edge time) ≥ current time
#             if v_type[u] == "station":
#                if edge_time >= cur_time and edge_time < earliest[v]:
#                    earliest[v] = edge_time
#                    parent[v] = (u,e)
#                    heapq.heappush(pq, (edge_time, v))

#             # trip -> station: arrival time (edge time) is new earliest arrival time at station
#             else:
#                if edge_time >= cur_time and edge_time < earliest[v]:
#                    earliest[v] = edge_time
#                    parent[v] = (u,e)
#                    heapq.heappush(pq, (edge_time,v))
# -------


# ----- fails
        # station -> trip: only take trip if dep time (edge time) ≥ current time
        # if v_type[u] == "station":
        #     if edge_time < cur_time:
        #         continue # trip not feasable
        #     candidate_time = edge_time
        
        # # trip -> station: arrival time (edge time) is new earliest arrival time at station
        # else: 
        #     #if edge_time < cur_time:
        #     #    continue 
        #     candidate_time = edge_time
        
        # # relaxation
        # if candidate_time < earliest[v]:
        #     earliest[v] = candidate_time
        #     parent[v] = (u,e)
        #     heapq.heappush(pq, (candidate_time, v))
# -----

    return earliest, parent

# build path from start to goal
def reconstruct_path(parent, start, goal):

    if goal not in parent and goal != start:
        return None

    path = []
    v = goal
    while v != start:
        path.append(v)
        if v not in parent:
            return None
        v = parent[v][0]
    path.append(start)
    return list(reversed(path))
