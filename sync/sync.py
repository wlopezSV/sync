#-------------------------------------------------------------------------------
# Name:         sync.py
# Purpose:      Comparar dos directorios localmente
#
# Author:      William Lopez
#
# Created:     03/05/2015
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#Importacion de Modulos
#-------------------------------------------------------------------------------
import os
import time
import sys
from filesys.dirs import clsFile
import sqlite3

#-------------------------------------------------------------------------------
#Variables Globales
#-------------------------------------------------------------------------------
verbose = True
log = True
logfile = "sync-"
database = "sync.db"
Job_Id = 0

#-------------------------------------------------------------------------------
#Definicion de Funciones
#-------------------------------------------------------------------------------
def CreaDataBase():
    db = sqlite3.connect(database)
    cur = db.cursor()
    sql = "create table Job(Id_Job Int Not Null, DirOrigen Text Not Null, DirDestino Text Not Null,LastSync Text Null)"
    cur.execute(sql)
    sql = "create table Job_Det(Id_Job Int Not Null, IsDir Int Not Null, IsFile Int Not Null,"
    sql += "NombreBaseOrigen Text Not Null,NombreOrigen Text Not Null,"
    sql += "NombreBaseDestino Text Not Null,NombreDestino Text Not Null)"
    cur.execute(sql)
    db.commit()
    db.close()

def GetIdJob(DirOrigen,DirDestino):
    global Job_Id

    db = sqlite3.connect(database)
    cur = db.cursor()
    cur.execute("""select count(Id_Job) from Job where DirOrigen = ? and DirDestino = ? """,(DirOrigen,DirDestino))
    count = cur.fetchone()[0]
    if count==0:
        Job_Id=1
        cur.execute("""Insert into Job (Id_Job,DirOrigen,DirDestino) Values(?,?,?)""",(Job_Id,DirOrigen,DirDestino))
        db.commit()
    else:
        cur.execute("""select Id_Job from Job where DirOrigen = ? and DirDestino = ? """,(DirOrigen,DirDestino))
        Job_Id = cur.fetchone()[0]
    db.close()


def CreaLog():
    FLog = open(logfile,"w")
    FLog.write("Log: " + os.path.basename( sys.argv[0] + "\n"))
    FLog.close

def printMSG(msg):
    global verbose
    if verbose:
        print msg
    if log:
        Flog = open(logfile,"a")
        Flog.write(msg+"\n")
        Flog.close

def CreaTree(DirOrigen,DirDestino,ObjDestino):
    printMSG("   CMD:")
    printMSG("        mkdir " + os.path.join(DirDestino,ObjDestino))
    for obj in os.listdir(os.path.join(DirOrigen,ObjDestino)):
        FullNameOrigen = os.path.join(DirOrigen,ObjDestino,obj)
        if os.path.isfile(FullNameOrigen):
            printMSG("        cp " + FullNameOrigen + " " + os.path.join(DirDestino,ObjDestino))
        if os.path.isdir(FullNameOrigen):
            CreaTree(os.path.join(DirOrigen,ObjDestino),os.path.join(DirDestino,ObjDestino),obj)

def Compara(DirOrigen,DirDestino):
    ''' Funcion que compara dos directorios recursivamente
    '''
    if not os.path.isdir(DirOrigen):
        printMSG(DirOrigen + " no es un directorio o no existe !!!")

    printMSG("Compara : [" + DirOrigen + "] --> [" + DirDestino + "]")
    for obj in os.listdir(DirOrigen):
        FullNameOrigen = os.path.join(DirOrigen,obj)
        FullNameDestino = os.path.join(DirDestino,obj)
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

        if os.path.isdir(os.path.join(DirOrigen,obj)):
            printMSG("   Dir Origen : [" + os.path.join(DirOrigen,obj) + "]")
            printMSG("   Dir Destino: [" + os.path.join(DirDestino,obj) + "]")
            printMSG("   Tarea:")
            if not os.path.isdir(os.path.join(DirDestino,obj)):
                printMSG("     - Directorio no existe, crear -> " + os.path.join(DirDestino,obj))
                CreaTree(DirOrigen,DirDestino,obj)
            else:
                printMSG("     - Directorio existe, revisar -> [" + os.path.join(DirOrigen,obj) + "]   &   [" + os.path.join(DirDestino,obj) + "]")
                Compara(os.path.join(DirOrigen,obj),os.path.join(DirDestino,obj))



def main():
    global verbose, logfile , log, database

    logfile = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"SYNC-" + time.strftime("%Y-%m-%d") + ".log")
    database = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"sync.db")

    verbose = False
    log = True

    if log:
        CreaLog()

    if not os.path.isfile(database):
        CreaDataBase()

    start_time = time.time()
    strTime = time.strftime("%H:%M:%S")
    printMSG("INICIO SYNC :" + strTime)
    if not verbose:
        print "INICIO SYNC :" + strTime

    DirOrigen = "C:\Datos Temporales\ASA2000"
    DirDestino = "C:\DatosTemp\ASA2000"

    GetIdJob(DirOrigen,DirDestino)
    Compara(DirOrigen,DirDestino)

    elapsed_time = time.time() - start_time
    strTime = time.strftime("%H:%M:%S")

    printMSG("FIN SYNC     :" + strTime)
    printMSG("Tiempo       :" + str(elapsed_time))
    if not verbose:
        print "FIN SYNC    :" + strTime
        print "Tiempo      :" + str(elapsed_time)
    if log:
        print "Log generado:" + logfile

if __name__ == '__main__':
    main()
