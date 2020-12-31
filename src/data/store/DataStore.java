/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package data.store;

import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Scanner;
import java.lang.Integer;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.json.simple.JSONObject;

public class DataStore {

    public static String  path="C:\\Users\\sharm\\Documents\\NetBeansProjects\\Data-Store\\src\\data\\store\\File\\f1.txt";
    /*YOU CAN RELACE YOUR PATH FOR STORING DATA-STORE FILE IN THE ABOVE LINE*/
    static int readWriteLock=0;
    synchronized public static void initialize_Datastore(){
        try{
		
			System.out.println("Do you want to choose your own folder to create DATA-STORE file:(Y/N)");
			Scanner scan=new Scanner(System.in);
			String opt=scan.nextLine();
			if(opt.equals("Y")){
				System.out.println("Enter file destination path: (Eg: C:\\"+"\\User\\"+"\\xyz\\"+"\\desktop\\"+"\\filename.txt) \"used double slash\""); 
				path=scan.nextLine();
			}
			File obj =new File(path);
			if(obj.createNewFile()){
				System.out.println("File Created: "+obj.getName());
			}else{
				System.out.println("File already exist.");
				System.out.println("File Size in bytes: "+obj.length());
			}
		}catch(IOException e){
			System.out.println("An error occured.");
			e.printStackTrace();
		}
    }
    
    synchronized public static int searchRecord(String filepath,String searchkey,String delimiter) {
		int pos=0;
		String currentline;
		String data[];
		int flag=0;
                
		try{
                    while(readWriteLock==1);
			FileReader fr=new FileReader(filepath);
			BufferedReader br=new BufferedReader(fr);
			while((currentline=br.readLine())!=null)
			{
				data=currentline.split("-");
				if((data[pos].equalsIgnoreCase(searchkey)))
				{
					flag=1;
				}	
			}
			fr.close();
			br.close();
		}catch(FileNotFoundException e){
			System.out.println(e);
		}catch(IOException e){
			System.out.println(e);
		}
		return flag;
	}
    
    synchronized public static void create_keyvaluepair(){
        try{
	    FileWriter obj =new FileWriter(path,true);
	    System.out.println("Enter new Key (CAPS-32 CHAR) : ");
	    Scanner scan=new Scanner(System.in);
	    String key=scan.nextLine();
	    if(key.length()!=32){
		System.out.println("Please enter the 32 character key");
	    }else{
	        int flag=searchRecord(path,key,"-");
		if(flag==1){
			System.out.println("Key already exists!! You can not create multiple value for a single key!!");
		}else{
                      while(readWriteLock==1){System.out.println("wait another client is writing");};
                       readWriteLock=1;
			obj.write("\n"+key+"-");
			String ch="";
			JSONObject jobj=new JSONObject();
			System.out.println("Enter Value as FIELD-DATA format (CAPS): ");
			do{
				System.out.print("Enter Field: ");
				String field=scan.nextLine();
				System.out.print("Enter Corressponding Data: ");
				String data=scan.nextLine();
				jobj.put(field,data);
                                System.out.print("Do you want to add more fields:(Y/N)");
				ch=scan.nextLine();
			}while(ch.equalsIgnoreCase("Y"));
			StringWriter value=new StringWriter();
			jobj.writeJSONString(value);
			String finalvalue=value.toString();
			obj.write(finalvalue);
			System.out.println("\nDo you want to set TIME-TO-LIVE property for this key-value pair: (Y/N)");
			String opt=scan.nextLine();
			if(opt.equalsIgnoreCase("Y")){
				obj.write("-1-");
				System.out.println("Enter Time-To-Live as no of seconds:");
			        String sec=scan.nextLine();
				obj.write(sec+"-");
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date=new Date();
				obj.write(sdf.format(date));
			}else{
				obj.write("-0");
                        }
			System.out.println("Successfully wrote to the DATA-STORE.");
		    }
		}
			readWriteLock=0;
			obj.close();
			
		}catch(IOException e){
			System.out.println("An error occured.");
			e.printStackTrace();
		}
    
    }
    
    synchronized public static void readRecord(String filepath,String searchkey,String delimiter) {
		int pos=0;
		String currentline;
		String data[];
		int flag=0;
		try{
                        while(readWriteLock==1);
			FileReader fr=new FileReader(filepath);
			BufferedReader br=new BufferedReader(fr);
			while((currentline=br.readLine())!=null)
			{
				data=currentline.split("-");
				if((data[pos].equalsIgnoreCase(searchkey)))
				{
                                    System.out.println("\n***********KEY-VALUE************\n");
					if(data[2].equals("0")){
					System.out.println("Key -> "+searchkey);
                                        System.out.println("Value:");
					String str[]=data[1].split("\"");
                                        int len=str.length;
                                        for(int i=1; i < len-1; i=i+4){
                                            System.out.println("\t"+str[i]+" -> "+str[i+2]);
                                        }
                                        System.out.println("JSON FORMAT Value -> "+data[1]);
					}else{
						long timetolive=new Long(data[3]);
						String d1=data[4];
						SimpleDateFormat sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
						Date date=new Date();
						String d2=sdf.format(date);
						Date dd1=sdf.parse(d1);
						Date dd2=sdf.parse(d2);
						long diff=dd2.getTime()-dd1.getTime();
						long livedtime=diff/1000;
						if(livedtime>timetolive){
							System.out.println("Sorry this key-value pair is Dead(Time to live is finished)\n");
						}else{
							System.out.println("Key -> "+searchkey);
							System.out.println("Value:");
                                                        String str[]=data[1].split("\"");
                                                        int len=str.length;
                                                         for(int i=1; i < len-1; i=i+4){
                                                             System.out.println("\t"+str[i]+" -> "+str[i+2]);
                                                         }
                                                      System.out.println("JSON FORMAT Value -> "+data[1]);
						}
					}
					flag=1;
				}
			}
			if(flag==0){
				System.out.println("!!!!!!!!!!Key does not exists!!!!!!!");
			}
			fr.close();
			br.close();
		}catch(FileNotFoundException e){
			System.out.println(e);
		}catch(IOException e){
			System.out.println(e);
		}catch(ParseException e){
		System.out.println(e);
		}
	}
    
    synchronized public static void removeRecord(String filepath,String removetext,int position,String delimiter){
		int pos=position-1;
		String tempFile="temp.txt";
		File oldfile=new File(filepath);
		File newfile=new File(tempFile);
		String currentline;
		String data[];
		int flag=0;
		try{
                    while(readWriteLock==1);
                    readWriteLock=1;
			FileWriter fw=new FileWriter(tempFile,true);
			BufferedWriter bw=new BufferedWriter(fw);
			PrintWriter pw=new PrintWriter(bw);
			FileReader fr=new FileReader(filepath);
			BufferedReader br=new BufferedReader(fr);
			while((currentline=br.readLine())!=null)
			{
				data=currentline.split("-");
				if(!(data[pos].equalsIgnoreCase(removetext)))
				{
					pw.println(currentline);
				}
				if((data[pos].equalsIgnoreCase(removetext))){
					if(data[2].equals("1")){
						long timetolive=new Long(data[3]);
						String d1=data[4];
						SimpleDateFormat sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
						Date date=new Date();
						String d2=sdf.format(date);
						Date dd1=sdf.parse(d1);
						Date dd2=sdf.parse(d2);
						long diff=dd2.getTime()-dd1.getTime();
						long livedtime=diff/1000;
						if(livedtime>timetolive){
							System.out.println("Sorry this key-value pair is Already Dead(Time to live is finished)\n");
							pw.println(currentline);
						}else{
							flag=1;
						}
					}else{
						flag=1;
					}
				}
			}
			pw.flush();
			pw.close();
			fr.close();
			br.close();
			bw.close();
			fw.close();
			oldfile.delete();
			File dump=new File(filepath);
			newfile.renameTo(dump);
                        readWriteLock=0;
			if(flag==0){
				System.out.println("!!!!!!!!!!Key does not exists!!!!!!!!");
			}else{
				System.out.println("Key-Value pair Deleted Successfully!!");
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}
    
    public static void main(String[] args) {
        // TODO code application logic here
        System.out.println("******************WELCOME TO DATA-STORE*********************");
        initialize_Datastore();
        System.out.println("******************DATA-STORE IS READY***********************");
        String ch="";
        do{
            System.out.println("Operation Menu\n Press 1 To Create a New Key-Value Pair\n Press 2 To Read from Data-Store\n Press 3 To Delete a Key-Value Pair from Data-Store");
            Scanner scan1=new Scanner(System.in);
            int opt=Integer.parseInt(scan1.nextLine());
            switch(opt){
                case 1:create_keyvaluepair();
                       break;
                case 2:System.out.println("Enter the key to read from data store: ");
		       String key =scan1.nextLine();
                       readRecord(path,key,"-");
                       break;
                case 3:System.out.println("Enter the key to read from data store: ");
		       String key1 =scan1.nextLine();
		       removeRecord(path,key1,1,"-");
                       break;
                default:System.out.println("Enter a valid option");
                        break;
            }
            System.out.println("\nDo you Want to continue?(Y/N)");
            ch=scan1.nextLine();
        }while(ch.equalsIgnoreCase("Y"));
        System.out.println("******************THANK YOU FOR VISITING DATA-STORE*********************");
    }
}
