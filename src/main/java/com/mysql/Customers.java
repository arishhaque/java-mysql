package com.mysql;

import java.sql.*;

public class Customers {

    public Connection connection;

    public Customers(Connection connection){
        this.connection = connection;
    }

    public void insert() {

        System.out.println("*******Insert into Customers*******");
        String[] customerId = {"SM-01", "RO-02", "TE-03", "ST-04", "JA-05", "EM-06", "HI-07", "SH-08", "RO-09"};
        String[] name = {"Smith", "Ronald", "Terry", "Steve", "Jack", "Emma", "Hillary", "Shane", "Robert"};
        String[] state = {"New York", "California", "Washington", "Florida", "California", "New york", "Colorado",
                            "Florida", "California"};
        int k = name.length;

        for(int i=0; i<k; i++) {
            try {
                String query = "insert into customers (customer_id, name, state) values(?,?,?)";
                PreparedStatement stmt = connection.prepareStatement(query);
                stmt.setString(1, customerId[i]);
                stmt.setString(2, name[i]);
                stmt.setString(3, state[i]);
                stmt.execute();

                System.out.println("Customer Added to the DB");

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println();
    }

    public void update() {

        System.out.println("*******Update Customer*******");
        try {
            String query = "update customers set state = ? where customer_id = ?";
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, "North Carolina");
            stmt.setString(2, "HI-07");
            int result = stmt.executeUpdate();
            if (result > 0) {
                System.out.println("Record Updated successfully");
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    public void delete(String customerId) {

        System.out.println("*******Delete Customer*******");
        try {
            String query = "delete from customers where customer_id = ?";
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, customerId);

            int result = stmt.executeUpdate();
            if (result > 0) {
                System.out.println("Customer Deleted successfully"); }

        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }


    public void selectAllCustomers() {

        System.out.println("*******Select All Customers*******");
        try {

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from customers");
            while (resultSet.next()) {

                System.out.print("Customer Id "+resultSet.getString("customer_id") +"   ");
                System.out.println("Customer Name "+resultSet.getString("name"));
            }

        }catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}
