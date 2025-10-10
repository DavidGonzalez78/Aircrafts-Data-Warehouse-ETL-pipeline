

# Preguntas



La función de extracción debería cargar todos los datos de AIMS y AMOS en la memoria? O debería haber una especie de flujo/pipeline, donde los datos se fueran pasando al transform y al load progresivamente? 
__No, debería haber un flujo hecho con iteradores__


Puedo cambiar la estructura del template?
__Preferiblemente no porque así corrige más fácil__



Puedo sacar con el extract solo lo que me importa? 
__Puedes hacer selects de columnas específicos y wheres, pero no hagas joints ni cosas complicadas__



# Tareas pendientes
- Entender cómo se calculan los KPIs a partir de las bases de datos originales.
- Hacer el transform.py
- Hacer las queries de los KPIs
- Ser capaz de añadir y ver elementos de la base de datos de DuckDB
- Hacer el load.py
