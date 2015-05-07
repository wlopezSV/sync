#-----------------------------------------------------------------------------------------------
# Name:         dirs.py
# Purpose:      Realizar operaciones administrativas sobre directorios
#
# Author:      William Lopez
#
# Created:     28/04/2015
#-----------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
#Importacion de Modulos
#-------------------------------------------------------------------------------
import os
import datetime
import glob

#-------------------------------------------------------------------------------
#Declaracion de variables y constantes
#-------------------------------------------------------------------------------
SUFFIXES = {1000: ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
        1024: ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']}

#-------------------------------------------------------------------------------
#Declaracion de funciones
#-------------------------------------------------------------------------------
def GetFileSizeStr(size, a_kilobyte_is_1024_bytes=True):
    '''
    Convert a file size to human-readable form.
    Arguments:
        size -- file size in bytes
        a_kilobyte_is_1024_bytes -- if True (default), use multiples of 1024
                                    if False, use multiples of 1000
    Returns: string
    '''
    if size < 0:
        raise ValueError('number must be non-negative')

    multiple = 1024.00 if a_kilobyte_is_1024_bytes else 1000.00
    for suffix in SUFFIXES[multiple]:
        size /= multiple
        if size < multiple:
            return '{0:.2f} {1}'.format(size, suffix)

    raise ValueError('number too large')

def Get_Mod_Date(fil):
    '''Obtiene la fecha de modificacion de un archivo o directorio
    Argumentos:
        fil -> Ruta y Nombre completo del archivo o directorio

    Returns: String
    '''
    t = os.path.getmtime(fil)
    MDate = datetime.datetime.fromtimestamp(t)
    return MDate.strftime('%d/%m/%Y %H:%M:%S')

def SearchInFiles(root,filter):
    ''' Funcion recursiva para buscar archivos dentro del directorio
    Argumentos :
        root -> Directorio raiz
        filter -> Filtro para buscar archivos
    '''
    TotalFiles = 0
    DirList = [ d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d)) ]
    cond = root + os.sep + filter
    FileList = glob.glob(cond)
    if len(FileList) > 0:
        print "[" + root + "] ->  " + str(len(FileList)) + " Found"
        TotalFiles = len(FileList)
    for files in FileList:
        print "    " + files
    if len(FileList) > 0:
        print " "
    for dirs in DirList:
        TotalFiles += SearchInFiles(os.path.join(root,dirs),filter)
    return TotalFiles

#-------------------------------------------------------------------------------
#Clase: clsFile - Manejo de Archivos
#-------------------------------------------------------------------------------
class clsFile(object):
    '''Clase para manejar las operaciones basicas sobre archivos
    '''
    def __init__(self,dir="",fil=""):
        self._validate_file(dir,fil)
        (FileName, exten) = os.path.splitext(fil)

        self.FullName = os.path.join(dir,fil)
        self.Name = fil
        self.BaseName = FileName
        self.ext  = exten
        self.Mod_Date = Get_Mod_Date(self.FullName)
        self.size_raw = os.path.getsize(self.FullName)
        self.size_str = GetFileSizeStr(self.size_raw)
        self.Mod_Datetime = os.path.getmtime(self.FullName)


    def _validate_file(self,dir,fil):
        if not os.path.isdir(dir):
            raise ValueError("dir no es un directorio")
        full = os.path.join(dir,fil)
        if not os.path.isfile(full):
            raise ValueError("No existe el archivo")

    def __str__(self):
        return self.Mod_Date + " " + self.size_str.rjust(12) + " " + self.Name

#-------------------------------------------------------------------------------
#Clase: clsDir - Manejo de Directorios
#-------------------------------------------------------------------------------
class clsDir(object):
    '''Clase para manejar las operaciones básicas sobre directorios
    '''
    def __init__(self,dir=""):
        if dir!="":
            self._validate_path(dir)
        self.PathDir = dir
        self.ParentDir = ""
        self.Dir = ""
        self.Mod_Date = Get_Mod_Date(dir)

    def _validate_path(self,dir):
        if not os.path.exists(dir):
            raise ValueError("dir must be a valid path ..." )
        if not os.path.isdir(dir):
            raise ValueError("dir must be a valid directory path ..." )

    def __str__(self):
        if self.Dir != "":
            return self.Mod_Date + " " +  " [" + self.Dir + "]"
        else:
            return self.Mod_Date + " " +  " [" + self.PathDir + "]"

