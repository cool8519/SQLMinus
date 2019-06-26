SQLMinus
=============================
Oracle의 SQL*Plus와 동일한 기능과 인터페이스를 가진 JAVA 기반의 Cross-DB 툴


Requirements
---------------
* JRE 1.5 이상

Getting Started
---------------
아래의 두개 버전 중 하나를 다운로드 한다.
* Windows Executable : [SQLMinus_v1.3.0.exe](https://github.com/cool8519/SQLMinus/raw/master/output/SQLMinus_v1.3.0.exe)
* Java Archive File : [SQLMinus_v1.3.0.jar](https://github.com/cool8519/SQLMinus/raw/master/output/SQLMinus_v1.3.0.jar)

Usage
---------------
1. SQLMinus.exe(jar)와 동일한 디렉토리에 JDBC Driver를 위치한다.
2. SQLMinus.exe(jar)를 실행시키면 접근 가능한 DBMS 목록이 나오며, 필요한 접속정보를 입력하면 연결된다.
   SQLMinus의 인자값으로 ConnectionURL을 주게 되면, 간단히 계정정보만 입력하면 연결된다.
   > 사용방법 확인 :<br>
      Windows> SQLMinus.exe -help <br>
      UNIX> java -jar SQLMinus.jar -help

Charset
---------------

  접속시 Input/Output Charset 정보를 입력하는데, 이는 SQL Query와, Result Data에 대한 엔코딩을 위한 값이다.<br>
 JAVA에서 지원하는 Charset을 지정하여 쿼리 입력시나, 조회시 한글 또는 특수문자에 대한 쿼리를 정상적으로 하기위해 필요하며, 특히 WAS등에서 어떤 encoding을 설정해야 하는지 확인시에 유용할 것으로 보인다.<br>
  > Default 값으로 사용하고자 하는 경우, 간단히 Enter를 입력하고<br>
    Encoding을 하지 않을 경우, '-' 을 입력하면 된다.

Commands
---------------

   대부분의 기능은 Oracle SQL*Plus를 기본으로 개발되었으며, Tibero tbSQL의 편리한 점도 접목하였다.<br>
   지원하는 기능은 아래와 같으며, 세부내용은 SQL*Plus 가이드를 참조하거나 help 명령어로 확인하기 바란다.<br>
   
* help<br>
    도움말 보기
* connect<br>
     타 계정 및 DB로의 연결을 위해 사용
* desc<br>
     해당 Object에 대한 Description을 확인
* spool<br>
     쿼리의 결과를 OS 파일로 기록
* edit<br>
     현재의 쿼리를 EDIT 화면에서 수정(현재 Windows만 지원)
* define / undefine<br>
   SQL*Minus 내의 사용자 변수를 저장하거나 삭제
* clear / cls<br>
     화면을 Clear
* /<br>
     이전(Buffer에 저장된) Query를 다시 수행
* !<br>
   OS 명령어를 수행
* list<br>
     이전(Buffer에 저장된) Query를 출력
* ls<br>
     현재 사용자의 Object 목록을 출력
* set (pagesize / heading / linesize / checksize / timing / time / scan / check / checkconn)<br>
   SQL*Minus의 시스템 변수를 지정하거나 확인
> * check 기능 <br>
   check 옵션이 ON일 경우, 쿼리의 결과가 checksize보다 크면 경고 메세지 출력
>  * checkconn 기능 <br>
     체크쿼리를 수행하여 connection이 정상인 지 확인하거나 유지
* column ( clear / format / heading / wrapped / truncated / on / off )<br>
   Query 출력을 위한 컬럼 정보를 지정하거나 확인
* &<br>
   Query 입력시 '&변수명'을 통한 사용자 변수 입력
* savepoint / commit / rollback<br>
   savepoint 명령을 통한 구간 지정 및 commit, rollback
* @<br>
      스크립트를 수행
* info (1.1.3~)<br>
      현재 Connection 정보를 출력
* version (1.1.3~)<br>
      프로그램, Driver, DBMS의 버전을 출력
* sleep (1.3.0~)<br>
      지정한 시간동안 멈춤
* print (1.3.0~)<br>
      특정 문자열을 화면에 출력

Supported DBMS
---------------
 - Oracle
 - Tibero
 - MS-SQL 2000
 - MS-SQL 2005
 - Sybase
 - Informix
 - DB2
 - MySQL
 - PostgreSQL
 - ALTIBASE
 - UNISQL
 - HSQLDB
 - POINTBASE

Release Note
---------------
1.1.4
 - 버그수정<br>
   > DB2 Property 추가시 URL 파싱 에러 수정<br>

1.1.5
 - 추가기능<br>
   > oracle sysdba 계정 접속 가능<br>
   > keep-alive(check-query)<br>
   
1.2.0
 - 추가기능<br>
   > 디버그 로깅<br>
   > 현재 디렉토리 하위의 모든 라이브러리를 동적으로 로딩<br>
   > 특정 라이브러리 위치 지정<br>

1.3.0
 - 버그수정<br>
   > 스크립트 내에서 다른 스크립트 호출시 상대경로 문제<br>
 - 추가기능<br>
   > sleep, print 명령<br>
   > WITH절 인식<br>
   > Single-line 주석(--)<br>
   > Multi-line 주석(/* */)<br>

Future Plan
---------------
* Procedure 지원
* Unix 환경에서 edit 지원
