package com.sds.tool.jdbc.sqlm.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import com.sds.tool.jdbc.sqlm.SQLMinus;
import com.sds.tool.util.PasswordThread;
import com.sds.tool.util.ToolLogger;


public class Util {

    private static final int SCREEN_LINE = 150;
    private static SimpleDateFormat defaultTimeFormat = new SimpleDateFormat("HH:mm:ss");


    private static void logln(Object msg, int level) {
        ToolLogger.logln(msg, level);
    }


    private static void log(Object msg, int level) {
        ToolLogger.log(msg, level);
    }


    public static String getCurrentTime() {
        return defaultTimeFormat.format(new java.util.Date());
    }


    public static String getCurrentTime(String format) {
        return (new SimpleDateFormat(format)).format(new java.util.Date());
    }


    public static String readLine(String prompt, int level) {
        StringBuffer buffer = new StringBuffer("");
        log(prompt, level);
        int c = ' ';

        try {

            do {
                c = System.in.read();
                buffer.append((char)c);
            } while(c != '\n');

            ToolLogger.logOnlySpool(buffer.toString(), false);

            return buffer.toString();

        } catch (IOException e) {
            return "";
        }
    }


    public static String readLine(int st_prom, char finalizer, boolean tm, int level) {
        StringBuffer buffer = new StringBuffer("");
        String prom;
        String line;
        String line_org;
        String finalizerStr = Character.toString(finalizer);
        char fin = '\n';
        int cnt = st_prom;
        int digit = 0;
        String tmstr = "";

        do {
            digit = String.valueOf(cnt).length();
            tmstr = (tm)?(getCurrentTime()+" "):"";
            prom = tmstr + getColumn(null, 3-digit, " ", "", true) + cnt++ + "  ";
            line_org = readLine(prom, level);
            line = line_org.trim();

            if(line.endsWith(finalizerStr)) {
                fin = finalizer;
                line_org = line_org.substring(0, line_org.lastIndexOf((int)finalizer));
            } else if(line.equals("")) {
                SQLMinus.sqlbuffer = buffer.toString().trim();
                return null;
            }

            buffer.append(line_org);
        } while(fin != finalizer);

        return buffer.toString();
    }


    public static String readPassword(String prompt) {
        PasswordThread et = new PasswordThread(prompt);
        Thread mask = new Thread(et);
        mask.start();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String password = "";

        try {
            password = in.readLine();
        } catch(IOException e) {
            System.out.println(e.getMessage().trim());
        }

        et.stopMasking();
        System.out.print("\010");

        return password;
    }


    public static boolean isNumber(String str) {
        boolean check = true;
        for(int i = 0; i < str.length(); i++) {
            if(!Character.isDigit(str.charAt(i))) {
                check = false;
                break;
            }
        }
        return check;
    }


    public static int getCharHowManyTimes(String str, char c) {
        int cnt = 0;
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) == c) {
                cnt++;
            }
        }
        return cnt;
    }


    public static boolean isIncludeEquals(String str, String essential, String optional) {
        if(essential.getBytes().length > str.getBytes().length) {
            return false;
        }

        String first = str.substring(0, essential.length());
        if(!first.equals(essential)) {
            return false;
        }

        String second = str.substring(essential.length());
        if(second.length() > 0) {
        	if(optional == null) {
        		return false;
        	} else {
                if(optional.getBytes().length < second.getBytes().length) {
                    return false;
                }
                if(optional.startsWith(second)) {
                    return true;
                } else {
                    return false;
                }
        	}
        } else {
        	return true;
        }
    }


    public static int getLineCount(String str) {
        int cnt = 1;
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) == '\n') {
                cnt++;
            }
        }
        return cnt;
    }


    public static String getLineData(String s, int line) {
        int prev_idx = -1;
        int idx = -1;
        int cnt = 0;
        String str = s + "\r\n";
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) == '\n') {
                cnt++;
                prev_idx = idx;
                idx = i;
            }
            if(cnt == line+1) {
                if(i > 0 && str.charAt(i-1) == '\r') {
                    return str.substring(prev_idx+1, idx-1);
                } else {
                    return str.substring(prev_idx+1, idx);
                }
            }
        }
        if(cnt == 0) {
            if(line == 0) {
                return str;
            } else {
                return "";
            }
        } else {
            return "";
        }
    }


    public static int indexOfList(List<Integer> lst, int num, int from) {
        int data = 0;
        for(int i = from; i < lst.size(); i++) {
            data = (lst.get(i)).intValue();
            if(data == num) {
                return i;
            }
        }
        return -1;
    }


    public static int getListIndex(List<Integer> colSize, int i) {
        int idx = 0;
        int listIdx = 0;
        int data = 0;
        while(idx <= i) {
            data = (colSize.get(listIdx)).intValue();
            if(data != -1) {
                idx++;
            }
            listIdx++;
        }
        return listIdx-1;
    }


    public static int getRealIndex(List<Integer> colSize, int i) {
        int idx = 0;
        int listIdx = 0;
        int data = 0;
        if(colSize.size()-1 < i) {
            return -1;
        }
        while(listIdx <= i) {
            data = (colSize.get(listIdx)).intValue();
            if(data != -1) {
                idx++;
            }
            listIdx++;
        }
        return idx-1;
    }


    public static void printLine(int size, int isNewline) {
        String end = "";
        if(isNewline < 0) end = "\n";
        if(isNewline > 0) end = " ";
        printColumn(null, size, "-", end, true);
    }


    public static String getData(String data, int size, int splitNum, int isNewline, boolean isLeft) {
        int initIdx = 0;
        int endIdx = 0;
        int bytelen = data.getBytes().length;
        String end = "";
        if(isNewline < 0) end = "\n";
        if(isNewline > 0) end = " ";
        String str = "";

        if(bytelen <= initIdx) {
            str = null;
        } else {
            if(isKor(data)) { // �ѱ� �� ���
                initIdx = toKorIndex(data, size, splitNum, true);
                endIdx = toKorIndex(data, size, splitNum, false);
            } else {
                if(splitNum > bytelen/size) {
                    initIdx = bytelen;
                    endIdx = bytelen;
                } else {
                    initIdx = splitNum * size;
                    endIdx = ((initIdx+size>bytelen)?bytelen:(initIdx+size));
                }
            }
            str = data.substring(initIdx, endIdx);
        }

        return getColumn(str, size, " ", end, isLeft);
    }


    public static boolean isKor(String str) {
        if(str.length() < str.getBytes().length) {
            return true;
        } else {
            return false;
        }
    }


    public static int getKorSplitSize(String str, int size) {
        int sum = 0;
        int line = 1;
        char[] sorts = getCharSorts(str);
        for(int i = 0; i < sorts.length; i++) {
            sum += (sorts[i]=='N' || sorts[i]=='E')?1:2;
            if(sum+1 == size) {
                if(i < sorts.length-1) {
                    if(sorts[i+1] == 'K') {
                        sum = 0;
                        line++;
                    }
                }
            } else if(sum == size) {
                sum = 0;
                if(i < sorts.length-1) {
                    line++;
                }
            }
        }
        return line;
    }


    public static char[] getCharSorts(String str) {
        char[] sorts = new char[str.length()];
        char c;
        for(int i = 0; i < str.length(); i++) {
            c = str.charAt(i);
            if(Character.isDigit(c)) {
                sorts[i] = 'N';
            } else if(c < 127) {
                sorts[i] = 'E';
            } else if(c > 255) {
                sorts[i] = 'K';
            }
        }
        return sorts;
    }


    public static int toKorIndex(String str, int size, int split, boolean isInit) {
        int sum = 0;
        int sp = 0;
        int korIdx = 1;
        int initIdx = 0;
        int line = getKorSplitSize(str, size);

        if(split == 0 && isInit) {
            return 0;
        }

        if((split+1 == line && !isInit) || split+1 > line) {
            return str.length();
        }

        char[] sorts = getCharSorts(str);
        for(int i = 0; i < sorts.length; i++) {
            sum += (sorts[i]=='N' || sorts[i]=='E')?1:2;
            if(sum+1 == size) {
                if(i < sorts.length-1) {
                    if(sp == split) {
                        if(sorts[i+1] != 'K') {
                            korIdx++;
                        }
                        return ((isInit)?initIdx:korIdx);
                    } else {
                        if(sorts[i+1] == 'K') {
                            sum = 0;
                            sp++;
                            initIdx = korIdx;
                        }
                    }
                } else {
                    return ((isInit)?initIdx:korIdx);
                }
            } else if(sum == size) {
                if(sp == split) {
                    return ((isInit)?initIdx:korIdx);
                }
                sum = 0;
                sp++;
                initIdx = korIdx;
            }
            korIdx++;
        }
        return ((isInit)?initIdx:str.length());
    }


    public static void printData(String data, int size, int splitNum, int isNewline, boolean isLeft) {
        log(getData(data, size, splitNum, isNewline, isLeft), ToolLogger.RESULT);
    }


    public static String getColumn(String colStr, int colLen, String blank, String endStr, boolean isLeft) {
        StringBuffer result = new StringBuffer("");
        colStr = (colStr==null)?"":colStr;

        if(isLeft) {
            result.append(colStr);
            for(int i = 0; i < (colLen-colStr.getBytes().length); i++) {
                result.append(blank);
            }
        } else {
            for(int i = 0; i < (colLen-colStr.getBytes().length); i++) {
                result.append(blank);
            }
            result.append(colStr);
        }
        if(endStr != null) {
            result.append(endStr);
        }
        return result.toString();
    }


    public static void printColumn(String colStr, int colLen, String blank, String endStr, boolean isLeft) {
        log(getColumn(colStr, colLen, blank, endStr, isLeft), ToolLogger.RESULT);
    }


    public static void clearScreen() {
        for (int i = 0; i < SCREEN_LINE; i++)
            logln("", ToolLogger.RESULT);
    }


    public static String getParameters(Hashtable<String,String> params, String first_deli, String deli) {
        return getParameters(params, first_deli, deli, false);
    }


    public static String getParameters(Hashtable<String,String> params, String first_deli, String deli, boolean end_deli) {
        String str = "";
        String k = "";
        Enumeration<String> e = params.keys();
        int cnt = 0;
        while(e.hasMoreElements()) {
            if(str.length() < 1) {
                str += first_deli;
            } else {
                str += deli;
            }
            k = e.nextElement();
            str += k + "=" + params.get(k);
            cnt++;
        }
        if(cnt > 0) {
            str += deli;
        }
        return str;
    }


    public static String getSumList(List<Integer> lst, boolean includeBlank) {
        String result = "";
        int sum = 0;
        int data = 0;
        for(int i = 0; i < lst.size(); i++) {
            data = (lst.get(i)).intValue();
            if(data != -1) {
                sum += data + ((includeBlank)?1:0);
            } else {
                result += String.valueOf(--sum) + " ";
                sum = 0;
            }
        }
        result += String.valueOf(--sum);
        return result;
    }


    public static String getColSizeString(List<Integer> lst) {
        StringBuffer result = new StringBuffer("");

        result.append("[ ");
        for(int i = 0; i < lst.size(); i++) {
            result.append(lst.get(i) + " ");
        }
        result.append("] ");
        result.append(getSumList(lst, true));
        return result.toString();
    }


    public static int getColCntInList(List<Integer> lst) {
        int cnt = 0;
        int data = 0;
        for(int i = 0; i < lst.size(); i++) {
            data = (lst.get(i)).intValue();
            if(data != -1) {
                cnt++;
            }
        }
        return cnt;
    }


    public static void initArray(String[] arr) {
        for(int i = 0; i < arr.length; i++) {
            arr[i] = "";
        }
    }


    public static String getStrExcludeChar(String str, char c) {
        StringBuffer result = new StringBuffer("");
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) != c) {
                result.append(str.charAt(i));
            }
        }
        return result.toString();
    }


    public static String replaceChar(String str, int idx, String ex, char c) {
        if(str.length() < idx) {
            return str;
        }
        int cnt = 0;
        StringBuffer result = new StringBuffer("");
        for(int i = 0; i < str.length(); i++) {
            if(!String.valueOf(str.charAt(i)).equals(ex)) {
                if(idx == cnt) {
                    result.append(c);
                } else {
                    result.append(str.charAt(i));
                }
                cnt++;
            } else {
                result.append(str.charAt(i));
            }
        }
        return result.toString();
    }


    public static String getPasswordString(String pass) {
        if(pass == null)
            return "";
        StringBuffer sb = new StringBuffer("");
        for(int i = 0; i < pass.length(); i++) {
            sb.append("*");
        }
        return sb.toString();
    }


    public static int indexOfExcludeChar(String str, int idx, char ex) {
        int cnt = -1;
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) != ex) {
                cnt++;
            }
            if(idx == cnt) {
                return i;
            }
        }
        return -1;
    }


    public static String substringWithDigit(String str, int begin, int end, char ex) {
        int begin_idx = indexOfExcludeChar(str, begin, ex);
        int end_idx = indexOfExcludeChar(str, end, ex);
        return str.substring(begin_idx, end_idx);

    }


    public static String checkFileName(String fname, String ext) {
        int idx = -1;
        char[] ca = System.getProperty("file.separator").toCharArray();
        idx = fname.lastIndexOf(ca[0]);
        String fn = (idx < 0)?fname:fname.substring(idx+1);
        if(fn.indexOf('.') < 0) {
            fn += "." + ext;
        }
        return (fname.substring(0, idx+1) + fn);
    }


    public static boolean isSpecialChar(char ch) {
        if( ((ch >= 0x02) && (ch <= 0x2F)) || ((ch >= 0x3A) && (ch <= 0x40)) || ((ch >= 0x5B) && (ch <= 0x60)) ) {
            return true;
        } else {
            return false;
        }
    }


    public static String getFirstSpecialChar(String str) {
        char ch;
        for(int i = 0; i < str.length(); i++) {
            ch = str.charAt(i);
            if(isSpecialChar(ch)) {
                return Character.toString(ch);
            }
        }
        return null;
    }


    public static String makePrintFormatString(String str, int size) {

        char ch;
        int from = 0;
        int cnt = 0;
        StringBuffer sb = new StringBuffer("");

        for(int i = 0; i < str.length(); i++) {
            ch = str.charAt(i);
            String chStr = Character.toString(ch);

            if(chStr.getBytes().length > 1) {
                cnt += 2;
            } else {
                cnt++;
            }

            if(cnt > size) {
                sb.append(str.substring(from, i) + " \n");
                cnt = 0;
                from = i;
                i--;
            } else if(cnt == size && i < str.length()-1) {
                sb.append(str.substring(from, i+1) + "\n");
                cnt = 0;
                from = i+1;
            } else if(ch == '\n') {
                sb.append(str.substring(from, i+1));
                cnt = 0;
                from = i+1;
            }
        }

        if(from < str.length()) {
            sb.append(str.substring(from));
        }

        return sb.toString();
    }


    public static String getDataFromList(Hashtable<String,String> ht, String k) {
        String var = null;
        String value = null;
        Enumeration<String> vars = ht.keys();
        while(vars.hasMoreElements()) {
            var = vars.nextElement();
            if(k.equals(var)) {
                value = ht.get(var);
            }
        }
        return value;
    }


    public static String replaceString(String content, String old_str, String new_str) {
        StringBuffer result = new StringBuffer("");
        int from = 0;
        int to = 0;

        while((to = content.indexOf(old_str, from)) > -1) {
            result.append(content.substring(from, to));
            result.append(new_str);
            from = to + old_str.length();
        }

        if(from == 0) {
            result.append(content);
        } else if(from < content.length()) {
            result.append(content.substring(from));
        }

        return result.toString();
    }


    public static void printListData(List<?> lst) {
        for(int i = 0; i < lst.size(); i++) {
            Object obj = lst.get(i);
            if(obj instanceof List) {
                printListData((List<?>)obj);
                if(i < lst.size()-1) {
                    logln("=============================================================", ToolLogger.RESULT);
                }
            } else {
                logln(obj, ToolLogger.RESULT);
                if(i < lst.size()-1) {
                    logln("--------------------------------------------", ToolLogger.RESULT);
                }
            }
        }
    }

}


