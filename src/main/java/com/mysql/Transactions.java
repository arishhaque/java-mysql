package com.mysql;

import java.sql.*;

public class Transactions {

    public Connection connection;

    public Transactions(Connection connection){
        this.connection = connection;
    }

    public void insert() {

        System.out.println("*******Insert into Transactions*******");
        String[] transactionIds = {"tr-01", "tr-02", "tr-03", "tr-04", "tr-05", "tr-06", "tr-07", "tr-08", "tr-09"};

        String[] customerIds = {"SM-01", "RO-02", "TE-03", "RO-02", "ST-04", "SM-01", "RO-02", "SM-01", "SM-01"};

        String[] status = {"complete", "complete", "complete", "inprogress", "complete", "complete", "complete",
                "inprogress", "inprogress"};

        String[] orderDate = {"2021-12-27", "2022-01-29", "2022-02-27", "2022-03-22", "2022-03-30", "2022-04-12",
                "2022-05-05", "2022-06-27", "2022-06-28"};

        String[] amount = {"1000", "200", "500", "802", "150", "600", "650", "420", "900"};

        int k = transactionIds.length;
        for(int i=0; i<k; i++) {

            try{
                String query = "INSERT INTO transactions (transaction_id, status, amount, customer_id,  completed_at) \n" +
                        "VALUES (?, ?, ?, ?, ?)";

                PreparedStatement stmt = connection.prepareStatement(query);
                stmt.setString(1, transactionIds[i]);
                stmt.setString(2, status[i]);
                stmt.setString(3, amount[i]);
                stmt.setString(4, customerIds[i]);
                stmt.setString(5, orderDate[i]);
                stmt.execute();
                System.out.println("Transaction Added");

            }catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println();
    }

    public void update() {

        System.out.println("*******Update Transaction*******");
        try {
            String query = "Update transactions set status = ? where transaction_id = ?";
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, "Completed");
            stmt.setString(2, "tr-08");
            int result = stmt.executeUpdate();
            if (result > 0) {
                System.out.println("Transaction Record Updated successfully");
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    public void delete(String transactionId) {

        System.out.println("*******Delete Transaction*******");
        try {
            String query = "delete from transactions where transaction_id = ?";
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, transactionId);

            int result = stmt.executeUpdate();
            if (result > 0) {
                System.out.println("Transaction Deleted successfully"); }

        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }


    public void selectAllTransactions() {

        System.out.println("*******Select All Transactions*******");
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from transactions");
            while (resultSet.next()) {

                System.out.print("Transaction Id "+resultSet.getString("transaction_id") +"   ");
                System.out.println("Customer Id "+resultSet.getString("customer_id"));
            }

        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}
