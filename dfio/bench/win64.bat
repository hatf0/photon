REM release
REM dmd -g -m64 -ofhello-dlang.exe -O -release -inline -I..\..\http-parser.d\lib static_http\hello.d ..\dfio.d ..\dfio_win.d ..\dfio_linux.d ..\..\http-parser.d\out\http-parser.lib
REM debug
dmd -m64 -ofhello-dlang.exe -debug -I..\..\http-parser.d\lib static_http\hello.d ..\dfio.d ..\dfio_win.d ..\dfio_linux.d ..\..\http-parser.d\out\http-parser.lib