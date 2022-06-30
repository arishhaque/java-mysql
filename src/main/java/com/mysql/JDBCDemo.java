package com.mysql;

import java.sql.*;

public class JDBCDemo {

    String url = "jdbc:mysql://localhost:3306/order_db";
    String user = "admin";
    String password = "admin@123";
    String driver = "com.mysql.jdbc.Driver";
    Connection connection;

    public void initConnection() {

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connection is Successful to the database" + url);

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {

        if(connection != null) {
            try {
                connection.close();
                System.out.println("Connection is Closed to the database" + url);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }



    public static void main(String[] args) {

        JDBCDemo jdbcDemo = new JDBCDemo();
        jdbcDemo.initConnection();

        Customers customer = new Customers(jdbcDemo.connection);
        customer.insert();
        customer.selectAllCustomers();
        customer.update();
        customer.delete("RO-09");
        customer.selectAllCustomers();

        Transactions transaction = new Transactions(jdbcDemo.connection);
        transaction.insert();
        transaction.selectAllTransactions();
        transaction.update();
        transaction.delete("tr-09");
        transaction.selectAllTransactions();


        Queries queries = new Queries(jdbcDemo.connection);
        queries.innerJoin();
        queries.leftJoin();
        queries.rightJoin();
        queries.crossJoin();
        queries.selfJoin();
        queries.groupByAndCount();
        queries.minAndMaxTransactionAmount();
        queries.sumAndAverageTransactionAmount();
        queries.firstTransaction();
        queries.lastTransaction();
        jdbcDemo.closeConnection();
    }
}
