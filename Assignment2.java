/* 
 * This code is provided solely for the personal and private use of students 
 * taking the CSC343H course at the University of Toronto. Copying for purposes 
 * other than this use is expressly prohibited. All forms of distribution of 
 * this code, including but not limited to public repositories on GitHub, 
 * GitLab, Bitbucket, or any other online platform, whether as given or with 
 * any changes, are expressly prohibited. 
*/ 

import java.sql.*;
import java.util.Date;
import java.util.Arrays;
import java.util.List;

public class Assignment2 {
   /////////
   // DO NOT MODIFY THE VARIABLE NAMES BELOW.
   
   // A connection to the database
   Connection connection;

   // Can use if you wish: seat letters
   List<String> seatLetters = Arrays.asList("A", "B", "C", "D", "E", "F");

   Assignment2() throws SQLException {
      try {
         Class.forName("org.postgresql.Driver");
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
      }
   }

  /**
   * Connects and sets the search path.
   *
   * Establishes a connection to be used for this session, assigning it to
   * the instance variable 'connection'.  In addition, sets the search
   * path to 'air_travel, public'.
   *
   * @param  url       the url for the database
   * @param  username  the username to connect to the database
   * @param  password  the password to connect to the database
   * @return           true if connecting is successful, false otherwise
   */
   public boolean connectDB(String URL, String username, String password) {
      // Implement this method!
      try{
         
         // establish connection
         System.out.println("Connecting as user: " + username);
         connection = DriverManager.getConnection(URL, username, password);
         System.out.println("Successfully Connected!");

         // set search path
         String queryString = "SET SEARCH_PATH TO air_travel, public";
         PreparedStatement pStatement = connection.prepareStatement(queryString);
         pStatement.executeUpdate();
         System.out.println("Set search_path to air_travel, public");
         
         return true;

      } catch (SQLException se){

         System.out.println("Error: " + se);

         return false;
      }

   }

  /**
   * Closes the database connection.
   *
   * @return true if the closing was successful, false otherwise
   */
   public boolean disconnectDB() {
      // Implement this method!
      try{

         connection.close();

         System.out.println("Bye~");

         return true;
      } catch (SQLException se){

         System.out.println("Error: " + se);

         return false;
      }
   }
   
   /* ======================= Airline-related methods ======================= */

   /**
    * Attempts to book a flight for a passenger in a particular seat class. 
    * Does so by inserting a row into the Booking table.
    *
    * Read handout for information on how seats are booked.
    * Returns false if seat can't be booked, or if passenger or flight cannot be found.
    *
    * 
    * @param  passID     id of the passenger
    * @param  flightID   id of the flight
    * @param  seatClass  the class of the seat (economy, business, or first) 
    * @return            true if the booking was successful, false otherwise. 
    */
   public boolean bookSeat(int passID, int flightID, String seatClass) {
      // Implement this method!
      try{
         // sanity check
         if (seatClass != "economy" && seatClass != "business" && seatClass != "first"){
            return false;
         }

         String queryString;
         PreparedStatement pStatement;
         ResultSet rs;

         // check the capacity of the flight
         queryString = "SELECT capacity_economy, capacity_business, capacity_first FROM Flight, Plane WHERE id = ? AND Flight.plane = tail_number";
         pStatement = connection.prepareStatement(queryString);
         pStatement.setInt(1, flightID);
         rs = pStatement.executeQuery();

         int Eco_capacity = 0;
         int Bus_capacity = 0;
         int First_capacity = 0;

         while (rs.next()){
            Eco_capacity = rs.getInt("capacity_economy");
            Bus_capacity = rs.getInt("capacity_business");
            First_capacity = rs.getInt("capacity_first");
         }
         
         System.out.printf("Economy Capacity: %d\nBusiness Capacity: %d\nFirst Capacity: %d\n", Eco_capacity, Bus_capacity, First_capacity);

         // find the current max booking id
         queryString = "SELECT max(id) FROM Booking";
         pStatement = connection.prepareStatement(queryString);
         rs = pStatement.executeQuery();
         
         int booking_id = 0;
         while (rs.next()){
            booking_id = rs.getInt("max");
         }
         booking_id += 1; // the id for new tuple

         // get the number of total seats booked
         queryString = "SELECT count(id) FROM Booking WHERE flight_id = ? AND seat_class = ?::seat_class";
         pStatement = connection.prepareStatement(queryString);
         pStatement.setInt(1, flightID);
         pStatement.setString(2, seatClass);
         rs = pStatement.executeQuery();

         int num_booked = 0;
         while (rs.next()){
            num_booked = rs.getInt("count");
         } 
         System.out.printf("Number of seats booked: %d\n", num_booked);

         if (seatClass == "business" || seatClass == "first"){
            
            // check whether there is empty seat
            if ((seatClass == "business" && num_booked >= Bus_capacity) 
            || (seatClass == "first" && num_booked >= First_capacity)){
               System.out.println("No more seat. Cannot finish the request.");
               return false;
            }

            // if there is emtpy seats

            //如果passID不在Passenger里？？？？？？？？

            // get the current price
            queryString = "SELECT " + seatClass + " FROM Price WHERE flight_id = ?";
            pStatement = connection.prepareStatement(queryString);
            pStatement.setInt(1, flightID);
            rs = pStatement.executeQuery();

            int price = 0;
            while (rs.next()){
               price = rs.getInt(seatClass);
            }

            System.out.printf("Price: %d\n", price);

            // calculate the seat number
            int row = 0;
            char letter;

            if (seatClass == "first"){
               row = num_booked / 6 + 1;
               letter = (char)('A' + num_booked % 6);
            } else{
               row = num_booked / 6 + (int)Math.ceil(First_capacity / 6.0) + 1;
               letter = (char)('A' + num_booked % 6);
            }

            System.out.printf("Booked seat at: %d%c\n", row, letter);

            // insert the booking information into table Booking
            queryString = "INSERT INTO Booking VALUES (?,?,?,?,?,?::seat_class,?,?)";
            pStatement = connection.prepareStatement(queryString);
            pStatement.setInt(1, booking_id);
            pStatement.setInt(2, passID);
            pStatement.setInt(3, flightID);
            pStatement.setTimestamp(4, getCurrentTimeStamp());
            pStatement.setInt(5, price);
            pStatement.setString(6, seatClass);
            pStatement.setInt(7, row);
            pStatement.setString(8, String.valueOf(letter));

            pStatement.executeUpdate();

            System.out.println("(" + booking_id + "," + passID + "," 
            + flightID + "," + getCurrentTimeStamp() + "," + price + "," + seatClass + "," + row + letter + ")");

            return true;
         } else if (seatClass == "economy"){
            // check whether there is empty seat
            if (num_booked - Eco_capacity >= 10){
               System.out.println("No more seat. Cannot finish the request.");
               return false;
            }

            // if there is emtpy seats

            // get the current price
            queryString = "SELECT " + seatClass + " FROM Price WHERE flight_id = ?";
            pStatement = connection.prepareStatement(queryString);
            pStatement.setInt(1, flightID);
            rs = pStatement.executeQuery();

            int price = 0;
            while (rs.next()){
               price = rs.getInt(seatClass);
            }

            System.out.printf("Price: %d\n", price);

            // insert the booking information into table Booking
            queryString = "INSERT INTO Booking VALUES (?,?,?,?,?,?::seat_class,?,?)";
            pStatement = connection.prepareStatement(queryString);
            pStatement.setInt(1, booking_id);
            pStatement.setInt(2, passID);
            pStatement.setInt(3, flightID);
            pStatement.setTimestamp(4, getCurrentTimeStamp());
            pStatement.setInt(5, price);
            pStatement.setString(6, seatClass);

            // calculate the seat number
            int row = 0;
            char letter;

            if (num_booked < Eco_capacity){
               row = num_booked / 6 
               + (int)Math.ceil(First_capacity / 6.0) 
               + (int)Math.ceil(Bus_capacity / 6.0) + 1;
               letter = (char)('A' + num_booked % 6);

               System.out.printf("Booked seat at: %d%c\n", row, letter);

               pStatement.setInt(7, row);
               pStatement.setString(8, String.valueOf(letter));

               pStatement.executeUpdate();
               System.out.println("(" + booking_id + "," + passID + "," 
               + flightID + "," + getCurrentTimeStamp() + "," + price + "," + seatClass + "," + row + letter + ")");

            } else{
               System.out.println("Overbooked. Cannot assign a seat.");
               pStatement.setNull(7, Types.INTEGER);
               pStatement.setNull(8, Types.CHAR);
               pStatement.executeUpdate();
               System.out.println("(" + booking_id + "," + passID + "," 
               + flightID + "," + getCurrentTimeStamp() + "," + price + "," + seatClass + "," + "NULL,NULL)");
            }

            return true;
         }
         return false;
      } catch (SQLException se){
         System.out.println("Error: " + se);
         return false;
      }

      
   }

   /**
    * Attempts to upgrade overbooked economy passengers to business class
    * or first class (in that order until each seat class is filled).
    * Does so by altering the database records for the bookings such that the
    * seat and seat_class are updated if an upgrade can be processed.
    *
    * Upgrades should happen in order of earliest booking timestamp first.
    *
    * If economy passengers are left over without a seat (i.e. more than 10 overbooked passengers or not enough higher class seats), 
    * remove their bookings from the database.
    * 
    * @param  flightID  The flight to upgrade passengers in.
    * @return           the number of passengers upgraded, or -1 if an error occured.
    */
   public int upgrade(int flightID) {
      // Implement this method!
      return -1;
   }


   /* ----------------------- Helper functions below  ------------------------- */

    // A helpful function for adding a timestamp to new bookings.
    // Example of setting a timestamp in a PreparedStatement:
    // ps.setTimestamp(1, getCurrentTimeStamp());

    /**
    * Returns a SQL Timestamp object of the current time.
    * 
    * @return           Timestamp of current time.
    */
   private java.sql.Timestamp getCurrentTimeStamp() {
      java.util.Date now = new java.util.Date();
      return new java.sql.Timestamp(now.getTime());
   }

   // Add more helper functions below if desired.


  
  /* ----------------------- Main method below  ------------------------- */

   public static void main(String[] args) {
      // You can put testing code in here. It will not affect our autotester.
      System.out.println("Running the code!");
      System.out.println("test");
      
      try{
         Assignment2 a2 = new Assignment2();
         a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-wangw222", "wangw222", "");
         a2.bookSeat(1, 5, "economy");
         a2.disconnectDB();
      } catch (SQLException se){
         System.out.println("??");
      }
      
   }

}
