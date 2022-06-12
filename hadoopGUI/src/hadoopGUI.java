import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class hadoopGUI {
    private JComboBox functionComboBox;
    private JButton browseButton;
    private JFormattedTextField inputTextField;
    private JButton startButton;
    private JButton browseJarButton;
    private JFormattedTextField jarTextField;
    private JPanel panelMain;
    private JButton pushToHDFSButton;

    DefaultListModel<String> model = new DefaultListModel<>();
    private JButton deleteFromHDFSButton;
    private JComboBox filesComboBox;
    private JButton saveFileToLocalButton;
    private JTextField outputTextField;
    private JTextArea textArea1;

    static String filePath;
    static String fileName;
    static String jarPath;
    static String outFile;
    public hadoopGUI() {
        int i  = 0;
        startButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                // todo start the function
                int i;
                i = functionComboBox.getSelectedIndex();
                outFile = outputTextField.getText();
                startFunction(i);

            }
        });
        browseButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                JFileChooser jfc = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                int returnValue = jfc.showOpenDialog(null);
                if(returnValue == JFileChooser.APPROVE_OPTION) {
                    File selectedFile = jfc.getSelectedFile();
                    inputTextField.setText(selectedFile.getAbsolutePath());
                    filePath = selectedFile.getAbsolutePath();
                    fileName = selectedFile.getName();
                }
            }
        });
        pushToHDFSButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                try{
                    System.out.println(filePath);
                    Process process = Runtime.getRuntime().exec("hadoop fs -put "+ filePath +" /user/hadoopuser/localfiles/");
                    printResults(process);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                filesComboBox.addItem(fileName);
            }
        });
        browseJarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                JFileChooser jfc = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                int returnValue = jfc.showOpenDialog(null);
                if(returnValue == JFileChooser.APPROVE_OPTION) {
                    File selectedFile = jfc.getSelectedFile();
                    jarTextField.setText(selectedFile.getAbsolutePath());
                    jarPath = selectedFile.getAbsolutePath();
                }
            }
        });
        saveFileToLocalButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                try {
                    Process process = Runtime.getRuntime().exec("hadoop fs -copyToLocal /user/hadoopuser/localfiles/" + filesComboBox.getSelectedItem().toString() + " /home/hadoopuser/hadoopfiles/");
                    printResults(process);
                }catch (Exception e){
                    e.printStackTrace();
                    }
                }
        });
        deleteFromHDFSButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                try{
                    Process process = Runtime.getRuntime().exec("hadoop fs -rm -r /user/hadoopuser/localfiles/"+filesComboBox.getSelectedItem().toString());
                    printResults(process);
                }
                catch (Exception e){
                e.printStackTrace();
                }
                filesComboBox.removeItemAt(filesComboBox.getSelectedIndex());
            }
        });

    }

    public static void printResults(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";

        while((line = reader.readLine())!= null){
         System.out.println(line);
        }
    }

    public static void startFunction(int i){
        switch (i){
            case 1:
                try {
                    Process process = Runtime.getRuntime().exec("hadoop jar " + jarPath + " AveragePrice /user/hadoopuser/input/new_used_cars_data.csv /user/hadoopuser/"+outFile);
//                    Process process2 = Runtime.getRuntime().exec("hadoop fs -cat /user/hadoopuser/"+outFile+"/part-r-00000 | tail");
//                    printResults(process);
                }catch (Exception e){
                    e.printStackTrace();
                }

                break;
            case 2:
                try {
                    Process process = Runtime.getRuntime().exec("hadoop jar " + jarPath + " Mode /user/hadoopuser/input/new_used_cars_data.csv /user/hadoopuser/"+outFile);
//                    Process process2 = Runtime.getRuntime().exec("hadoop fs -cat /user/hadoopuser/"+outFile+"/part-r-00000 | tail");
//                    printResults(process);
                }catch (Exception e){
                    e.printStackTrace();
                }

                break;
            case 3:
                try {
                    Process process = Runtime.getRuntime().exec("hadoop jar " + jarPath + " BrandCounter /user/hadoopuser/input/new_used_cars_data.csv /user/hadoopuser/"+outFile);
//                    Process process2 = Runtime.getRuntime().exec("hadoop fs -cat /user/hadoopuser/"+outFile+"/part-r-00000 | tail");
//                    printResults(process);
                }catch (Exception e){
                    e.printStackTrace();
                }
                break;
            case 4:
                try {
                    Process process = Runtime.getRuntime().exec("hadoop jar " + jarPath + " Range /user/hadoopuser/input/new_used_cars_data.csv /user/hadoopuser/"+outFile);
//                    Process process2 = Runtime.getRuntime().exec("hadoop fs -cat /user/hadoopuser/"+outFile+"/part-r-00000 | tail");
//                    printResults(process);
                }catch (Exception e){
                    e.printStackTrace();
                }
                break;
            case 5:
                try {
                    Process process = Runtime.getRuntime().exec("hadoop jar " + jarPath + " StandardDeviation /user/hadoopuser/input/new_used_cars_data.csv /user/hadoopuser/"+outFile);
//                    Process process2 = Runtime.getRuntime().exec("hadoop fs -cat /user/hadoopuser/"+outFile+"/part-r-00000 | tail");

                }catch (Exception e){
                    e.printStackTrace();
                }
                break;

        }
    }

    public static void main(String[] args) {

        JFrame frame = new JFrame("APP");
        frame.setContentPane(new hadoopGUI().panelMain);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
