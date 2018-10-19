
# SQLMinus
 
Oracle의 SQL*Plus와 동일한 기능과 인터페이스를 가진 JAVA 기반의 Cross-DB 툴


## 구동환경

JDK 1.5 이상

## 지원 DBMS

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


## 사용방법

   1. SQLMinus.exe(jar)와 동일한 디렉토리에 JDBC Driver를 위치한다.
   2. SQLMinus.exe(jar)를 실행시키면 접근 가능한 DBMS 목록이 나오며, 필요한 접속정보를 입력하면 연결된다.
      SQLMinus의 인자값으로 ConnectionURL을 주게 되면, 간단히 계정정보만 입력하면 연결된다.
      > 사용방법 확인 :<br>
        Windows> SQLMinus.exe -help <br>
        UNIX> java -jar SQLMinus.jar -help

## Charset

   접속시 Input/Output Charset 정보를 입력하는데, 이는 SQL Query와, Result Data에 대한 엔코딩을 위한 값이다.
   JAVA에서 지원하는 Charset을 지정하여 쿼리 입력시나, 조회시 한글 또는 특수문자에 대한 쿼리를 정상적으로
    하기위해 필요하며, 특히 WAS등에서 어떤 encoding을 설정해야 하는지 확인시에 유용할 것으로 보인다.
   > Default 값으로 사용하고자 하는 경우, 간단히 Enter를 입력하고
      Encoding을 하지 않을 경우, '-' 을 입력하면 된다.

## 기능

   대부분의 기능은 Oracle SQL*Plus를 기본으로 개발되었으며, Tibero tbSQL의 편리한 점도 접목하였다.
   지원하는 기능은 아래와 같으며, 세부내용은 SQL*Plus 가이드를 참조하거나 help 명령어로 확인하기 바란다.
   1. connect
      타 계정 및 DB로의 연결을 위해 사용
   2. desc
      해당 Object에 대한 Description을 확인
   3. spool
      쿼리의 결과를 OS 파일로 기록
   4. edit
      현재의 쿼리를 EDIT 화면에서 수정(현재 Windows만 지원)
   5. define / undefine
      SQL*Minus 내의 사용자 변수를 저장하거나 삭제
   6. clear / cls
      화면을 Clear
   7. /
      이전(Buffer에 저장된) Query를 다시 수행
   8. !
       OS 명령어를 수행
   9. list
      이전(Buffer에 저장된) Query를 출력
   10. ls
      현재 사용자의 Object 목록을 출력
   11. set (pagesize / heading / linesize / checksize / timing / time / scan / check / checkconn)
      SQL*Minus의 시스템 변수를 지정하거나 확인
> * check 기능 <br>
    check 옵션이 ON일 경우, 쿼리의 결과가 checksize보다 크면 경고 메세지 출력
>  * checkconn 기능 <br>
    체크쿼리를 수행하여 connection이 정상인 지 확인하거나 유지
   12. column ( clear / format / heading / wrapped / truncated / on / off )
      Query 출력을 위한 컬럼 정보를 지정하거나 확인
   13. &
      Query 입력시 '&변수명'을 통한 사용자 변수 입력
   14. savepoint / commit / rollback
      savepoint 명령을 통한 구간 지정 및 commit, rollback
   15. @
      스크립트를 수행
   16. info (1.1.3~)
      현재 Connection 정보를 출력
   17. version (1.1.3~)
      프로그램, Driver, DBMS의 버전을 출력
   18. help
      도움말 보기

## 향후 지원 계획
   1. Procedure 지원
   2. Unix 환경에서 edit 지원
