package com.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Queries {

    public Connection connection;

    public Queries(Connection connection){
        this.connection = connection;
    }


    public void innerJoin() {

        System.out.println("**************************************************");
        System.out.println("Inner Join to fetch Customer Names for whom Transaction is completed");

        String query = "SELECT c.name, t.transaction_id, t.amount\n" +
                "from `customers` c INNER JOIN `transactions` t ON\n" +
                "c.customer_id=t.customer_id where t.status ='complete'";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String CustomerName = rs.getString(1);
                String transactionId = rs.getString(2);
                int amount =  rs.getInt(3);
                System.out.println("CustomerName: "+CustomerName +", TransactionId: "+transactionId+
                        " Amount: "+amount);
            }
        } catch (SQLException e) {
        e.printStackTrace();
        }
    }


    public void leftJoin() {

        System.out.println("**************************************************");
        System.out.println("LEFT JOIN to fetch all customers with transactions and Order By name");

        String query = "SELECT c.name, t.transaction_id, t.amount\n" +
                "from `customers` c LEFT JOIN `transactions` t ON\n" +
                "c.customer_id=t.customer_id ORDER BY c.name";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String CustomerName = rs.getString(1);
                String transactionId = rs.getString(2);
                int amount =  rs.getInt(3);
                System.out.println("CustomerName: "+CustomerName +", TransactionId: "+transactionId+
                        " Amount: "+amount);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rightJoin() {

        System.out.println("**************************************************");
        System.out.println("Right JOIN to fetch all transactions with customer Names and Order By name");

        String query = "SELECT c.name, t.transaction_id, t.amount\n" +
                "from `customers` c RIGHT JOIN `transactions` t ON\n" +
                "c.customer_id=t.customer_id ORDER BY c.name";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String CustomerName = rs.getString(1);
                String transactionId = rs.getString(2);
                int amount =  rs.getInt(3);
                System.out.println("CustomerName: "+CustomerName +", TransactionId: "+transactionId+
                        " Amount: "+amount);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void crossJoin() {

        System.out.println("**************************************************");
        System.out.println("Cross Join Query");
        String query = "SELECT distinct c.name, t.transaction_id, t.amount from `customers` c CROSS JOIN `transactions` t";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                String CustomerName = rs.getString(1);
                String transactionId = rs.getString(2);
                int amount =  rs.getInt(3);
                System.out.println("CustomerName: "+CustomerName +", TransactionId: "+transactionId+
                        " Amount: "+amount);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void selfJoin() {

        System.out.println("**************************************************");
        System.out.println("Self Join Query to fetch customers of same states");
        String query = "SELECT c1.name AS customerName1, c2.name AS customerName2, c1.state AS state\n" +
                "FROM customers c1, customers c2\n" +
                "WHERE c1.customer_id <> c2.customer_id\n" +
                "AND c1.state = c2.state";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                String customerName1 = rs.getString(1);
                String customerName2 = rs.getString(2);
                String state = rs.getString(3);
                System.out.println("CustomerName1: "+customerName1 +", CustomerName2: "+customerName2+
                        "       State: "+state);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void groupByAndCount() {

        System.out.println("**************************************************");
        System.out.println("Group By and Count Query to count customers of same states");
        String query = "SELECT state, count(customer_id) as count from `customers` GROUP BY state";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                String state = rs.getString(1);
                int count = rs.getInt(2);
                System.out.println("State: "+state +", Count: "+count);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void minAndMaxTransactionAmount() {

        System.out.println("**************************************************");
        System.out.println("Fetch Min and Max Amount Transactions");
        String query = "SELECT min(amount) as minAmountTransaction, max(amount) as maxAmountTransaction from `transactions`";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                int minAmount = rs.getInt(1);
                int maxAmount = rs.getInt(2);
                System.out.println("Minimum Amount Transaction: "+minAmount +", Maximum Amount Transaction: "+maxAmount);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void sumAndAverageTransactionAmount() {

        System.out.println("**************************************************");
        System.out.println("Fetch Total and Average Amount of all Transactions");
        String query = "SELECT sum(amount) as totalAmountTransaction, avg(amount) as avgAmountTransaction from `transactions`";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                int totalAmount = rs.getInt(1);
                double avgAmount = rs.getDouble(2);
                System.out.println("Total Amount Transaction: "+totalAmount +", Average Amount Transaction: "+avgAmount);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void firstTransaction() {

        System.out.println("**************************************************");
        System.out.println("Fetch First Transaction Using order by date");
        String query = "SELECT * From `transactions` order by completed_at asc LIMIT 1";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                String transactionId = rs.getString(1);
                String status = rs.getString(2);
                int amount = rs.getInt(3);
                String customerId = rs.getString(2);
                String transactionDate = rs.getString(2);
                System.out.println("TransactionId: "+transactionId +", Status: "+status+
                        ", Amount: "+amount+ ", customerId: "+customerId+", transactionDate"+transactionDate);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void lastTransaction() {

        System.out.println("**************************************************");
        System.out.println("Fetch Last Transaction Using order by date");
        String query = "SELECT * From `transactions` order by completed_at desc LIMIT 1";
        System.out.println("-----------Query--------------");
        System.out.println(query);
        System.out.println();
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){

                String transactionId = rs.getString(1);
                String status = rs.getString(2);
                int amount = rs.getInt(3);
                String customerId = rs.getString(2);
                String transactionDate = rs.getString(2);
                System.out.println("TransactionId: "+transactionId +", Status: "+status+
                        ", Amount: "+amount+ ", customerId: "+customerId+", transactionDate"+transactionDate);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
