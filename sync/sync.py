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
import shutil

#-------------------------------------------------------------------------------
#Variables Globales
#-------------------------------------------------------------------------------
verbose = True
log = True
logfile = "sync-"
database = "sync.db"
Id_Job = 0
LastSync=""
FirstSync = True

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

def GetJob(DirOrigen,DirDestino):
    global Id_Job,LastSync,FirstSync

    db = sqlite3.connect(database)
    cur = db.cursor()
    cur.execute("""select count(Id_Job) from Job where DirOrigen = ? and DirDestino = ? """,(DirOrigen,DirDestino))
    count = cur.fetchone()[0]
    if count==0:
        Id_Job = 1
        FirstSync = True
        cur.execute("""Insert into Job (Id_Job,DirOrigen,DirDestino) Values(?,?,?)""",(Id_Job,DirOrigen,DirDestino))
        db.commit()
    else:
        FirstSync = False
        cur.execute("""select Id_Job,LastSync from Job where DirOrigen = ? and DirDestino = ? """,(DirOrigen,DirDestino))
        row = cur.fetchone()
        Id_Job = row[0]
        if row[1] != "None":
            LastSync = row[1]
    db.close()

def UpdateLastSync():
    global Id_Job, LastSync

    db = sqlite3.connect(database)
    cur = db.cursor()
    cur.execute("""Update Job Set LastSync = ? where Id_Job = ? """,(LastSync,Id_Job))
    db.commit()
    db.close()

def InsertDet(IsDir,IsFile,NombreBaseOrigen,NombreOrigen,NombreBaseDestino,NombreDestino):
    global Id_Job
    try:
        db = sqlite3.connect(database)
        cur = db.cursor()
        db.text_factory = str
        cur.execute("""Insert into Job_Det (
                        Id_Job,
                        IsDir,
                        IsFile,
                        NombreBaseOrigen,
                        NombreOrigen,
                        NombreBaseDestino,
                        NombreDestino)
                        Values (?,?,?,?,?,?,?) """,
                        (Id_Job,IsDir,IsFile,NombreBaseOrigen,NombreOrigen,NombreBaseDestino,NombreDestino))
        db.commit()
    except sqlite3.Error, e:
        print "Error %s:" % e.args[0]

    db.close()

def BorraDet():
    global Id_Job

    db = sqlite3.connect(database)
    cur = db.cursor()
    cur.execute("""Delete from Job_Det Where Id_Job = ? """ , (Id_Job,))
    db.commit()
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

def BuscaCambiosEnOrigen(DirOrigen):
    global Id_Job

    printMSG("Busca cambios en : [" + DirOrigen + "]" )
    db = sqlite3.connect(database)
    db.text_factory = str
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute("""select IsDir,
                    IsFile,
                    NombreBaseOrigen,
                    NombreOrigen,
                    NombreBaseDestino,
                    NombreDestino
                    from Job_Det
                    where Id_Job = ? """,(Id_Job,))
    results = cur.fetchall()
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
    db.close()

def cpFile(src,dst):
    try:
        shutil.copy2(src,dst)
        printMSG("        Archivo " + src + " copiado." )
    except shutil.Error as e:
        printMSG("ERROR - cpFile: Archivo " + src + " no copiado. Error: %s" % e)

def rmFile(src):
    try:
        os.remove(src)
        printMSG("        Archivo " + src + " eliminado." )
    except shutil.Error as e:
        printMSG("ERROR - rmFile: Archivo " + src + " no eliminado. Error: %s" % e)

def cpDir(src,dst):
    try:
        os.mkdir(dst)
        shutil.copystat(src,dst)
        printMSG("        Directorio " + src + " copiado." )
    except shutil.Error as e:
        printMSG("ERROR - cpDir: Directorio " + src + " no copiado. Error: %s" % e)

def cpStatDir(src,dst):
    try:
        shutil.copystat(src,dst)
        printMSG("        Directorio " + src + " metadata copiada." )
    except shutil.Error as e:
        printMSG("ERROR - cpStatDir: Directorio " + src + " metadata no copiada. Error: %s" % e)

def rmDir(src):
    try:
        os.rmdir(src)
        printMSG("        Directorio " + src + " eliminado." )
    except shutil.Error as e:
        printMSG("ERROR - rmDir: Directorio " + src + " no eliminado. Error: %s" % e)

def Compara(DirOrigen,DirDestino):
    ''' Funcion que compara dos directorios recursivamente
    '''
    if not os.path.isdir(DirOrigen):
        printMSG(DirOrigen + " no es un directorio o no existe !!!")

    printMSG("Compara : [" + DirOrigen + "] --> [" + DirDestino + "]")
    for obj in os.listdir(DirOrigen):
        FullNameOrigen = os.path.join(DirOrigen,obj)
        FullNameDestino = os.path.join(DirDestino,obj)
        InsertDet(
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
                Compara(os.path.join(DirOrigen,obj),os.path.join(DirDestino,obj))



def main():
    global verbose, logfile , log, database, LastSync

    logfile = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"SYNC-" + time.strftime("%Y-%m-%d") + ".log")
    database = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),"sync.db")

    verbose = False
    log = True

    if log:
        CreaLog()

    if not os.path.isfile(database):
        CreaDataBase()

    DirOrigen = "C:\Datos Temporales\ASA2000"
    DirDestino = "C:\DatosTemp\ASA2000"

    GetJob(DirOrigen,DirDestino)
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
        BuscaCambiosEnOrigen(DirOrigen)
        BorraDet()

    Compara(DirOrigen,DirDestino)

    elapsed_time = time.time() - start_time
    strTime = time.strftime("%H:%M:%S")
    LastSync = time.strftime("%Y-%m-%d %H:%M:%S")

    printMSG("FIN SYNC     :" + strTime)
    printMSG("Tiempo       :" + str(elapsed_time))
    if not verbose:
        print "FIN SYNC    :" + strTime
        print "Tiempo      :" + str(elapsed_time)

    UpdateLastSync()

    if log:
        print "Log generado:" + logfile

if __name__ == '__main__':
    main()
