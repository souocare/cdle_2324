@echo off
echo off

set LibraryPath=..\NativeLibs

set JarFile=.\target\Demo01-OpenCV-ExtractFramesFromVideo-2020.2021.SemInv.jar

set Input=..\..\..\input\videos\faces.mp4
set Output=..\..\..\output\OpenCV\faces

mkdir %Output%

set Arguments=%Input% %Output%

set JavaBin=%JAVA_HOME%\bin\java

set JavaOptions=-Djava.library.path=%LibraryPath%

set Command=%JavaBin% %JavaOptions% -jar %JarFile% %Arguments%

echo.
echo %Command%
echo.
%Command%
echo.

pause