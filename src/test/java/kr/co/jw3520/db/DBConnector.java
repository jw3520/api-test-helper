package kr.co.jw3520.db;

import org.junit.Assert;

import java.sql.*;

public class DBConnector {
    /**
     *<b>BILL Insert 시, 누락된 컬럼 수 반환</b>
     *<pre>
     *     String[] requiredArr = {"A", "B"}
     *     DBConnector.billInsertCheck(requiredArr, "1234");
     *</pre>
     *
     * @param requiredArr
     *        검증할 컬럼 목록
     * @param TID
     *        거래건 TID
     * @return 누락된 컬럼의 수
     */
    public static void billInsertCheck(String[] requiredArr, String TID) {
        String query = createSelectQuery("BILL", requiredArr, Condition.newCondition("TID", TID));
        int count = 0;

        try(Connection conn = connect();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {
            if(resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                String column = "";
                String value = "";
                for(int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    column = resultSetMetaData.getColumnClassName(i);
                    value = resultSet.getString(i);
                    if(value == null || "".equals(value)) {
                        System.err.println(column + "은(는) 누락되었습니다.");
                        count++;
                    }
                }
            }
            if(count != 0) {
                throw new Exception("column required fail : " + count);
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     *<b>BILL Insert 시, 기대값과 일치하지 않는 컬럼 수 반환</b>
     *<pre>
     *     DBConnector.billInsertCheck(Condition.newCondition("A", "100"), "1234");
     *     DBConnector.billInsertCheck(Condition.newCondition("A", "100").addCondition("B", "abc"), "1234");
     *</pre>
     * @param condition
     *        조건(컬럼명, 값)
     * @param TID
     *        거래건 TID
     * @return 기대값과 일치하지 않는 컬럼의 수
     */
    public static void billInsertCheck(Condition condition, String TID) {
        String query = createSelectQuery("BILL", condition.getConditionKeyArr(), condition);
        int count = 0;

        try(Connection conn = connect();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {
            if(resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                String column = "";
                String value = "";
                for(int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    column = resultSetMetaData.getColumnClassName(i);
                    value = resultSet.getString(i);
                }
                if(!value.equals(condition.getValue(column))) {
                    System.err.println(column + "의 기대값(" + condition.getValue(column) + ")과 입력값(" + value + ")이 다릅니다.");
                    count++;
                }
            }
            if(count != 0) {
                throw new Exception("column condition fail : " + count);
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * <b>DB 테이블에서 조건에 맞는 컬럼의 데이터를 반환</b>
     *<pre>
     * String a = "100";
     * DBConnector.execute("BILL", "B", Condition.newCondition("A", a));
     *</pre>
     *
     * @param table
     * @param column
     * @param condition
     *        조건(컬럼명, 값)
     * @return 조건에 해당하는 데이터
     */
    public static String execute(String table, String column, Condition condition) {
        String query = createSelectQuery(table, column, condition);

        try(Connection conn = connect();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {
            if(resultSet.next()) {
                return resultSet.getObject(column).toString();
            }
            throw new SQLDataException("조회 결과 조건과 일치하는 데이터가 없습니다.");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return null;
    }

    private static Connection connect() throws Exception {
        String driver = "driverName";
        String url = "url";
        String userName = "userName";
        String password = "password";

        Class.forName(driver);
        return DriverManager.getConnection(url, userName, password);
    }

    private static String createSelectQuery(String table, String column, Condition condition) {
        return "SELECT " + column + " FROM " + table + " WHERE " + condition.getQuery() + ";";
    }

    private static String createSelectQuery(String table, String[] columns, Condition condition) {
        StringBuilder columnStr = new StringBuilder();

        for(String column : columns) {
            if(column.length() > 1 && !columns[0].equals(column)) {
                columnStr.append(',');
            }
            columnStr.append(column);
        }
        return "SELECT " + columnStr.toString() + " FROM " + table + " WHERE " + condition.getQuery() + ";";
    }
}
