import pandas as pd
import sqlalchemy
import os


#Parametros de conexion a bd destino
destination_host = 'localhost'
destination_database = 'interno_gc_acc'
destination_user = 'postgres'
destination_password = '12345'
destination_port = '5433'

# Connectar a la base de datos de destino usando SQLAlchemy
try:
    destination_engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{destination_user}:{destination_password}@{destination_host}:{destination_port}/{destination_database}')
    connDestination = destination_engine.connect()
except Exception as e:
    print(f"Error al conectarse a la base de destino: {e}")


schemas = ['cun25019','cun25035', 'cun25436', 'cun25040', 'cun25053', 'cun25095'] 

#Funcion para generar los modelos fisicos en base de datos apartir de los modelos INTERLIS bajo el estandar LADM 
#     Parametros de entrada:
#        - type_db [str] - type database: postgres or gpkg
#        - operation [str] - type operation INTERLIS: generate or export 
#        - schema [str] - Schema of BD
#        - name_xtf [str] Optional -  Name of XTF file result

#    Rutas para el jar de interlis y los modelos ili
#        jar ili2pg: "D:\ACC\desarrollos\ladm\internov1_a_internov2\ili2pg\ili2pg-4.4.3.jar"        
#        ruta ctm12_pg: "D:\ACC\desarrollos\ladm\internov1_a_internov2\ctm12\insert_ctm12_pg.sql"        
#        ruta modedelos: "D:\ACC\desarrollos\ladm\internov1_a_internov2\modelo_acc_interno" 
#        ruta logs: "D:\ACC\desarrollos\ladm\internov1_a_internov2\log"   

def generateInterlis(type_db,operation,schema,name_xtf = None):    
    try:
        if type_db == 'postgres':
            if operation == 'generate':
                cmd = 'java -jar D:\ACC\desarrollos\ladm\internov1_a_internov2\ili2pg\ili2pg-4.4.3.jar --schemaimport --dbhost {} --dbport {} --dbusr {} --dbpwd {} --dbdatabase {} --dbschema {} --setupPgExt --coalesceCatalogueRef --createEnumTabs --createNumChecks --coalesceMultiSurface --coalesceMultiLine --coalesceMultiPoint --coalesceArray --beautifyEnumDispName --createUnique --createGeomIdx --createFk --createFkIdx --createMetaInfo --expandMultilingual --createTypeConstraint --createEnumTabsWithId --createTidCol --smart2Inheritance --strokeArcs --defaultSrsCode 9377 --preScript D:\ACC\desarrollos\ladm\internov1_a_internov2\ctm12\insert_ctm12_pg.sql --log D:\ACC\desarrollos\ladm\internov1_a_internov2\log\logfile.txt D:\ACC\desarrollos\ladm\internov1_a_internov2\modelo_acc_interno\Modelo_Interno_ACC_V3.ili'.format(destination_host, destination_port,destination_user,destination_password,destination_database,schema)            
        os.system(cmd)
    except Exception as e:
        print(e)


for schema in schemas:
    generateInterlis('postgres','generate',schema)

connDestination.close()