#-------------------------------------------------------------------------------
# Name:         syncutils.py
# Purpose:      Funciones extra utilizadas por sync.py
#
# Author:      William Lopez
#
# Created:     09/05/2015
#-------------------------------------------------------------------------------
''' Módulo syncutils funciones auxiliares para el paquete base sync.py

    Funciones:
        CreaDataBase(database)
        GetJob(database,DirOrigen,DirDestino)
        IsFirstSync(database,DirOrigen,DirDestino)
        GetLastSync(database,DirOrigen,DirDestino)
        UpdateLastSync(database,Id_Job,LastSync)
        InsertDet(database,Id_Job,IsDir,IsFile,NombreBaseOrigen,NombreOrigen,NombreBaseDestino,NombreDestino)
        BorraDet(database,Id_Job)
'''
__author__      = "William Lopez"
__copyright__ = ""
__credits__ = ["", "", "",""]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "William Lopez"
__email__ = "wlopez.a@gmail.com"
__status__ = "Development"


#-------------------------------------------------------------------------------
#Importacion de Modulos
#-------------------------------------------------------------------------------
from db_utils.dbutils import clsSQLite

#-------------------------------------------------------------------------------
#Definicion de Funciones
#-------------------------------------------------------------------------------
def CreaDataBase(database):
    ''' Crea la base de datos que es usada por sync para guardar los directorios
    y archivos del directorio origen. Esto es con el objetivo de detectar en
    futuras syncronizaciones si hay algún archivo y/o folder que fue eliminado del
    Origen para replicar la eliminación al destino.

    Argumentos:
        database <string> : Ruta y nombre de la base de datos sqlite3
    '''
    db = clsSQLite(database)
    sql = "create table Job(Id_Job Int Not Null, DirOrigen Text Not Null, DirDestino Text Not Null,LastSync Text Null)"
    db.execsql(sql)
    sql = "create table Job_Det(Id_Job Int Not Null, IsDir Int Not Null, IsFile Int Not Null,"
    sql += "NombreBaseOrigen Text Not Null,NombreOrigen Text Not Null,"
    sql += "NombreBaseDestino Text Not Null,NombreDestino Text Not Null)"
    db.execsql(sql)

def GetJob(database,DirOrigen,DirDestino):
    ''' Obtiene el Id del Job de sincronizacion almacenado en la base de datos sqlite asociado al directorio
    origen y destino. Si no existe el registro en la tabla es insertado.

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        DirOrigen <string>  : Ruta completa al directorio origen
        DirDestino <string> : Ruta completa al directorio Destino

    Retorna:
        <string> : El Id_Job asociado a DirOrigen y DirDestino
    '''
    Id_Job = 1
    db = clsSQLite(database)
    count = db.getresults("""select count(Id_Job) from Job where DirOrigen = ? and DirDestino = ? """,
                            (DirOrigen,DirDestino),
                            True)
    if count==0:
        Id_Job = 1
        db.execsql("""Insert into Job (Id_Job,DirOrigen,DirDestino) Values(?,?,?)""",(Id_Job,DirOrigen,DirDestino))
    else:
        Id_Job = db.getresults("""select Id_Job from Job where DirOrigen = ? and DirDestino = ? """,
                                (DirOrigen,DirDestino),
                                True )
    return Id_Job

def IsFirstSync(database,DirOrigen,DirDestino):
    ''' Destermina si es la primera sincronización que se ejecuta entre los directorios
    origen y destino.

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        DirOrigen <string>  : Ruta completa al directorio origen
        DirDestino <string> : Ruta completa al directorio Destino

    Retorna:
        <boolean> : True si es la primera sincronizacion, False en caso contrario
    '''
    db = clsSQLite(database)
    count = db.getresults("""select count(Id_Job) from Job where DirOrigen = ? and DirDestino = ? """,
                            (DirOrigen,DirDestino),
                            True)
    if count==0:
        FirstSync = True
    else:
        FirstSync = False
    return FirstSync

def GetLastSync(database,DirOrigen,DirDestino):
    ''' Obtiene la fecha de la ultima sincronización que se ejecuto entre los directorios
    origen y destino.

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        DirOrigen <string>  : Ruta completa al directorio origen
        DirDestino <string> : Ruta completa al directorio Destino

    Retorna:
        <string> : La Fecha de la ultima sincronización
    '''
    db = clsSQLite(database)
    LastSync = db.getresults("""select LastSync from Job where DirOrigen = ? and DirDestino = ? """,
                            (DirOrigen,DirDestino),
                            True)
    return LastSync

def UpdateLastSync(database,Id_Job,LastSync):
    ''' Actualiza la tabla Job con la fecha y hora de la sincronizacion

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        Id_Job <int>        : Id del Job
        LastSync <string>   : Fecha y hora de la syncronizacion
    '''
    db = clsSQLite(database)
    db.execsql("""Update Job Set LastSync = ? where Id_Job = ? """,(LastSync,Id_Job))

def InsertDet(database,Id_Job,IsDir,IsFile,NombreBaseOrigen,NombreOrigen,NombreBaseDestino,NombreDestino):
    ''' Agrega el registro en la tabla Job_Det del direcotrio/archivo que se compara, esto es necesario
    para determinar en futuras sincronizaciones si se ha eliminado algun archivo

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        Id_Job <int>        : Id del Job que se esta ejecutando
        IsDir <boolean>     : Indica si el objeto es un directorio
        IsFile <boolean>    : Indica si el objeto es un archivo
        NombreBaseOrigen <string>   : Nombre base del directorio/archivo
        NombreOrigen <string>       : Nombre del directorio o archivo
                                    FullPath es NombreBaseOrigen+NombreOrigen
        NombreBaseDestino <string>  : Nombre base del directorio/archivo
        NombreDestino <string>      : Nombre del directorio o archivo
                                    FullPath es NombreBaseDestino+NombreDestino
    '''
    db = clsSQLite(database)
    db.execsql("""Insert into Job_Det (
                    Id_Job,
                    IsDir,
                    IsFile,
                    NombreBaseOrigen,
                    NombreOrigen,
                    NombreBaseDestino,
                    NombreDestino)
                    Values (?,?,?,?,?,?,?) """,
                    (Id_Job,IsDir,IsFile,NombreBaseOrigen,NombreOrigen,NombreBaseDestino,NombreDestino))


def BorraDet(database,Id_Job):
    ''' Borra el detalle del job antes de sincronizar ya que los registros vuelven a ser ingresados

    Argumentos:
        database <string>   : Ruta y nombre de la base de datos sqlite3
        Id_Job <int>        : Id del Job que se esta ejecutando
    '''
    db = clsSQLite(database)
    db.execsql("""Delete from Job_Det Where Id_Job = ? """ , (Id_Job,))


