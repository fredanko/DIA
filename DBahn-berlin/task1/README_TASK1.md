## Ausführreihenfolge der Notebooks
Folgende Ausführungsreihenfolge muss in der ETL Pipeline berücksichtigt werden:

1. table_station.ipynb
2. table_stop_planned.ipynb
3. table_stop_changed.ipynb

### table_station.ipynb
Richtet den Table mit allen Stationen, Namen, EVA-Nummer, und Lat/Long (Koordinaten) ein

### table_stop_planned.ipynb
Richtet den Table mit allen Stops ((XML Nummer + Stop ID) als PK, EVA als FK, ar_pt als geplante Ankunft, dp_pt als geplante Abfahrtszeit ein).

Da in den timetable.XML leider keine EVA Nummern vorhanden sind, muss das Matching zu den Stations leider über den Stationsnamen erfolgen. Das wird im Notebook über eine Art Jaccard-Änhlichkeit der Strings gelöst. Außerdem wird bei jeder Station "Berlin" entfernt, um Probleme wie "Berlin Alexanderplatz" vs. "Alexanderplatz" aufzulösen.

Außerdem muss der PK aus der Stop ID ((`<s id="...">` im XML) im XML) **und** der XML Nummer (bspw. '2509260400/berlin-westkreuz_timetable.xml') erfolgen. Das Problem ist nämlich, dass die Stop ID nicht über alle XML Dateien unique ist.

### table_stop_changed.ipynb
Fügt dem Stops-Table die Attribute actual_arrival und actual_departure zu, falls es zu Verspätungen kommt. Da der PK der Stops-Tabelle aus XML Nummer und Stop ID besteht und die Changes viertelstündlich, die Timetables aber stündlich in XMLs festgehalten sind, muss beim Matchen der Verspätungen zu den tatsächlichen Timetables darauf geachtet werden, die XML Nummer anzupassen (bspw. 25092604**15**/berlin-westkreuz_**change**.xml nach 25092604**00**/berlin-westkreuz_**timetable**.xml).


## Weitere Allgemein Remarks

- Vermutlich ist es sinnvoll, den SQL Code, der im Python Code einfach als String innerhalb von ``cur.execute("...")`` eingefügt wird, in ``.sql`` Dateien auszulagern, welche dann in den Python Notebooks gelagert weden. Das könnte die Abgabge schöner machen

## TODO

- Cancelation Times auch aus den XMLs rausholen
- Datenstrukturen für das Konstruieren eines Graphen aus den XMLs rausholen
- Ganz am Ende: Code cleanen