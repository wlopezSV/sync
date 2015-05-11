#-------------------------------------------------------------------------------
# Name:         sync.py
# Purpose:      Comparar dos directorios localmente
#
# Author:      William Lopez
#
# Created:     03/05/2015
#-------------------------------------------------------------------------------
''' Módulo principal sync : Sincronizacion de archivos y directorios

    Funciones:

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
import os
import time
import sys
import shutil

from filesys.dirs import clsFile
from db_utils.dbutils import clsSQLite
from syncutils import CreaDataBase
from syncutils import GetJob
from syncutils import UpdateLastSync
from syncutils import InsertDet
from syncutils import BorraDet
from syncutils import GetJob
from syncutils import IsFirstSync
from syncutils import GetLastSync
#-------------------------------------------------------------------------------
#Variables Globales
#-------------------------------------------------------------------------------
verbose = True
log = True
logfile = "sync-"

#-------------------------------------------------------------------------------
#Definicion de Funciones
#-------------------------------------------------------------------------------
def CreaLog():
    ''' Funcion para crear el log del modulo '''
    FLog = open(logfile,"w")
    FLog.write("Log: " + os.path.basename( sys.argv[0] + "\n"))
    FLog.close

def printMSG(msg):
    ''' Funcion para escribir los mensajes del modulo, si verbose es True los mensajes son
    escritos en pantalla. Si log es True los mensajes son escritos en el log '''
    global verbose
    if verbose:
        print msg
    if log:
        Flog = open(logfile,"a")
        Flog.write(msg+"\n")
        Flog.close

def CreaTree(DirOrigen,DirDestino,ObjDestino):
    ''' Crea recursivamente el arbol de directorios en DirDestino y copia todos los archivos
    de DirOrigen a DirDestino

    Argumentos:
        DirOrigen <string>      : Path base del directorio origen
        DirDestino <string>     : Path base del directorio destino a sincronizar
        ObjDestino <string>     : Contiene el nombre del directorio o el nombre del archivo
                                    FullPathOrigen  = DirOrigen + ObjDestino
                                    FullPathDestino = DirDestino + ObjDestino
    '''
    printMSG("   CMD:")
    printMSG("        mkdir " + os.path.join(DirDestino,ObjDestino))
    cpDir(os.path.join(DirOrigen,ObjDestino),os.path.join(DirDestino,ObjDestino))
    for obj in os.listdir(os.path.join(DirOrigen,ObjDestino)):
        FullNameOrigen = os.path.join(DirOrigen,ObjDestino,obj)
        if os.path.isfile(FullNameOrigen):
            printMSG("        cp " + FullNameOrigen + " " + os.path.join(DirDestino,ObjDestino))
            cpFile(FullNameOrigen,os.path.join(DirDestino,ObjDestino))
        if os.path.isdir(FullNameOrigen):
            CreaTree(os.path.join(DirOrigen,ObjDestino),os.path.join(DirDestino,ObjDestino),obj)
    cpStatDir(os.path.join(DirOrigen,ObjDestino),os.path.join(DirDestino,ObjDestino))

def BorraTree(DirDestino,ObjDestino):
    ''' Borra recursivamente el arbol de directorios en DirDestino y todos sus archivos

    Argumentos:
        DirDestino <string>     : Path base del directorio destino a sincronizar
        ObjDestino <string>     : Contiene el nombre del directorio o el nombre del archivo
                                    FullPathDestino = DirDestino + ObjDestino
    '''
    for obj in os.listdir(os.path.join(DirDestino,ObjDestino)):
        FullNameOrigen = os.path.join(DirDestino,ObjDestino,obj)
        if os.path.isfile(FullNameOrigen):
            printMSG("        rm " + FullNameOrigen )
            rmFile(FullNameOrigen)
        if os.path.isdir(FullNameOrigen):
            BorraTree(os.path.join(os.path.join(DirDestino,ObjDestino),obj))

    printMSG("   CMD:")
    printMSG("        rmdir " + os.path.join(DirDestino,ObjDestino))
    rmDir(os.path.join(DirDestino,ObjDestino))

def BuscaCambiosEnOrigen(database,Id_Job,DirOrigen):
    ''' Busca los cambios realizados en DirOrigen desde la ultima sincronizacion. Si encuentra que un
    archivo y/o directorio fue eliminado replica la eliminacion en DirDestino.

    Argumentos:
        database <string>       : Path y nombre de la base de datos sqlite3
        Id_Job <int>            : Id del Job de sincronizacion
        DirOrigen <string>      : Path base del directorio origen
    '''
    printMSG("Busca cambios en : [" + DirOrigen + "]" )
    db = clsSQLite(database)
    results = db.getresults("""select IsDir,
                    IsFile,
                    NombreBaseOrigen,
                    NombreOrigen,
                    NombreBaseDestino,
                    NombreDestino
                    from Job_Det
                    where Id_Job = ? """,(Id_Job,))
    for row in results:
        FullNameOrigen = os.path.join(row["NombreBaseOrigen"],row["NombreOrigen"])
        FullNameDestino = os.path.join(row["NombreBaseDestino"],row["NombreDestino"])
        if row[1]:
            if not os.path.isfile(FullNameOrigen):
                printMSG("   Fil Origen : " + FullNameOrigen)
                printMSG("   Fil Destino: " + FullNameDestino)
                printMSG("   Tarea:")
                printMSG("     - Archivo origen ya no existe, eliminar  -> [" + FullNameDestino + "]")
                printMSG("   CMD:")
                printMSG("        rm " + FullNameDestino)
                rmFile(FullNameDestino)

        if row[0]:
            if not os.path.isdir(FullNameOrigen):
                printMSG("   Dir Origen : [" + FullNameOrigen + "]")
                printMSG("   Dir Destino: [" + FullNameDestino + "]")
                printMSG("   Tarea:")
                printMSG("     - Directorio ya no existe, eliminar -> " + FullNameDestino )
                BorraTree(row["NombreBaseDestino"],row["NombreDestino"])

def cpFile(src,dst):
    ''' Copia archivo src a dst

    Argumentos:
        src <string>    : Fullpath del archivo origen
        dst <string>    : Fullpath donde sera copiado el archivo
    '''
    try:
        shutil.copy2(src,dst)
        printMSG("        Archivo " + src + " copiado." )
    except shutil.Error as e:
        printMSG("ERROR - cpFile: Archivo " + src + " no copiado. Error: %s" % e)

def rmFile(src):
    ''' Elimina el archivo src

    Argumentos:
        src <string>    : Fullpath del archivo a eliminar
    '''
    try:
        os.remove(src)
        printMSG("        Archivo " + src + " eliminado." )
    except shutil.Error as e:
        printMSG("ERROR - rmFile: Archivo " + src + " no eliminado. Error: %s" % e)

def cpDir(src,dst):
    ''' Copia el directorio src a dst. El copiado se realiza de la siguiente forma: 1. Crea el directorio
    en dst y 2. Copia la metadata de Fecha de creación y permisos al directorio creado para ambos tanto el
    origen como el destino tengan la misma data.

    Argumentos:
        src <string>    : Fullpath del directorio origen
        dst <string>    : Fulppath del directorio destino
    '''
    try:
        os.mkdir(dst)
        shutil.copystat(src,dst)
        printMSG("        Directorio " + src + " copiado." )
    except shutil.Error as e:
        printMSG("ERROR - cpDir: Directorio " + src + " no copiado. Error: %s" % e)

def cpStatDir(src,dst):
    ''' Copia los metadatos como Fecha de Creacion, modificacion y permisos del directorio src al dst

    Argumentos:
        src <string>    : Fullpath del directorio origen
        dst <string>    : Fullpath del directorio destino
    '''
    try:
        shutil.copystat(src,dst)
        printMSG("        Directorio " + src + " metadata copiada." )
    except shutil.Error as e:
        printMSG("ERROR - cpStatDir: Directorio " + src + " metadata no copiada. Error: %s" % e)

def rmDir(src):
    ''' Elimina el directorio src

    Argumentos:
        src <string>    : Fullpath del directorio a eliminar
    '''
    try:
        os.rmdir(src)
        printMSG("        Directorio " + src + " eliminado." )
    except shutil.Error as e:
        printMSG("ERROR - rmDir: Directorio " + src + " no eliminado. Error: %s" % e)

def Compara(DirOrigen,DirDestino,database,Id_Job):
    ''' Funcion que compara dos directorios recursivamente.

    Argumentos:
        DirOrigen <string>      : Fullpath del directorio origen. Directorio master.
        DirDestino <string>     : Fullpath del directorio a sincronizar. Directorio replicado
        database <stirng>       : Fullpath y nombre de la base de datos sqlite3
        Id_Job <int>            : Id del Job de sincronizacion
    '''
    if not os.path.isdir(DirOrigen):
        printMSG(DirOrigen + " no es un directorio o no existe !!!")

    printMSG("Compara : [" + DirOrigen + "] --> [" + DirDestino + "]")
    for obj in os.listdir(DirOrigen):
        FullNameOrigen = os.path.join(DirOrigen,obj)
        FullNameDestino = os.path.join(DirDestino,obj)
        InsertDet(database,Id_Job,
            os.path.isdir(FullNameOrigen),
            os.path.isfile(FullNameOrigen),
            DirOrigen,
            obj,
            DirDestino,
            obj)
        if os.path.isfile(FullNameOrigen):
            F_Origen = clsFile(DirOrigen,obj)
            printMSG("   Fil Origen : " + FullNameOrigen)
            printMSG("   size       : " + F_Origen.size_str)
            printMSG("   Fecha M.   : " + F_Origen.Mod_Date)
            if not os.path.isfile(FullNameDestino):
                printMSG("   Fil Destino: " + FullNameDestino)
                printMSG("   size       : ")
                printMSG("   Fecha M.   : ")
                printMSG("   Tarea:")
                printMSG("     - Archivo no existe, copiar a -> [" + DirDestino + "]")
                printMSG("   CMD:")
                printMSG("        cp " + FullNameOrigen + " " + DirDestino)
                cpFile(FullNameOrigen,DirDestino)
            else:
                F_Destino = clsFile(DirDestino,obj)
                Copiar = False
                printMSG("   Fil Destino: " + FullNameDestino)
                printMSG("   size       : " + F_Destino.size_str)
                printMSG("   Fecha M.   : " + F_Destino.Mod_Date)
                printMSG("   Tarea:")
                printMSG("     - Archivo existe en [" + DirDestino + "]")

                if F_Origen.size_raw > F_Destino.size_raw:
                    printMSG("     - Size Origen > Size Destino, copiar a -> [" + DirDestino + "]")
                    Copiar = True
                if F_Origen.size_raw < F_Destino.size_raw:
                    printMSG("     - Size Origen < Size Destino")
                if F_Origen.size_raw == F_Destino.size_raw:
                    printMSG("     - Size Origen = Size Destino")

                if F_Origen.Mod_Datetime > F_Destino.Mod_Datetime:
                    printMSG("     - Archivo origen es mas reciente, copiar a -> [" + DirDestino + "]")
                    Copiar = True
                if F_Origen.Mod_Datetime < F_Destino.Mod_Datetime:
                    printMSG("     - Archivo destino es mas reciente")
                if F_Origen.Mod_Datetime == F_Destino.Mod_Datetime:
                    printMSG("     - Ambos archivos tienen la misma fecha de modificacion")

                if Copiar:
                    printMSG("   CMD:")
                    printMSG("        cp " + FullNameOrigen + " " + DirDestino)
                    rmFile(FullNameDestino)
                    cpFile(FullNameOrigen,DirDestino)

        if os.path.isdir(os.path.join(DirOrigen,obj)):
            printMSG("   Dir Origen : [" + os.path.join(DirOrigen,obj) + "]")
            printMSG("   Dir Destino: [" + os.path.join(DirDestino,obj) + "]")
            printMSG("   Tarea:")
            if not os.path.isdir(os.path.join(DirDestino,obj)):
                printMSG("     - Directorio no existe, crear -> " + os.path.join(DirDestino,obj))
                CreaTree(DirOrigen,DirDestino,obj)
            else:
                printMSG("     - Directorio existe, revisar -> [" + os.path.join(DirOrigen,obj) + "]   &   [" + os.path.join(DirDestino,obj) + "]")
                Compara(os.path.join(DirOrigen,obj),os.path.join(DirDestino,obj),database,Id_Job)



def main():
    global verbose, logfile , log

    logfile = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"SYNC-" + time.strftime("%Y-%m-%d") + ".log")
    database = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"sync.db")

    verbose = False
    log = True

    if log:
        CreaLog()

    DirOrigen = "C:\Datos Temporales\ASA2000"
    DirDestino = "C:\DatosTemp\ASA2000"

    if not os.path.isfile(database):
        CreaDataBase(database)
        LastSync=""
        FirstSync = True
    else:
        FirstSync = IsFirstSync(database,DirOrigen,DirDestino)
        LastSync = GetLastSync(database,DirOrigen,DirDestino)

    Id_Job = GetJob(database,DirOrigen,DirDestino)

    if LastSync != "":
        printMSG("LAST SYNC   :" + LastSync)
        if not verbose:
            print "LAST SYNC   :" + LastSync

    start_time = time.time()
    strTime = time.strftime("%H:%M:%S")
    printMSG("INICIO SYNC :" + strTime)
    if not verbose:
        print "INICIO SYNC :" + strTime

    if not FirstSync:
        BuscaCambiosEnOrigen(database,Id_Job,DirOrigen)
        BorraDet(database,Id_Job)

    Compara(DirOrigen,DirDestino,database,Id_Job)

    elapsed_time = time.time() - start_time
    strTime = time.strftime("%H:%M:%S")
    LastSync = time.strftime("%Y-%m-%d %H:%M:%S")

    printMSG("FIN SYNC     :" + strTime)
    printMSG("Tiempo       :" + str(elapsed_time))
    if not verbose:
        print "FIN SYNC    :" + strTime
        print "Tiempo      :" + str(elapsed_time)

    UpdateLastSync(database,Id_Job,LastSync)

    if log:
        print "Log generado:" + logfile

if __name__ == '__main__':
    main()
