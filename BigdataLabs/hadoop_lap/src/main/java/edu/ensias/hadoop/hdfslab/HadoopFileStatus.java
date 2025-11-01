package edu.ensias.hadoop.hdfslab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileStatus {
    // public static void main(String[] args) {
    //     // Créer une configuration Hadoop
    //     Configuration conf = new Configuration();
    //     FileSystem fs;
        
    //     try {
    //         // Obtenir le système de fichiers HDFS
    //         fs = FileSystem.get(conf);
            
    //         // Chemin du fichier dans HDFS
    //         Path filepath = new Path("/user/root/input", "purchases.txt");
            
    //         // Vérifier si le fichier existe
    //         if (!fs.exists(filepath)) {
    //             System.out.println("File does not exist");
    //             System.exit(1);
    //         }
            
    //         // Obtenir les informations du fichier
    //         FileStatus status = fs.getFileStatus(filepath);
            
    //         // Afficher les informations
    //         System.out.println("=== Informations du Fichier ===");
    //         System.out.println("File Name: " + filepath.getName());
    //         System.out.println("File Size: " + status.getLen() + " bytes");
    //         System.out.println("File Owner: " + status.getOwner());
    //         System.out.println("File Group: " + status.getGroup());
    //         System.out.println("File Permission: " + status.getPermission());
    //         System.out.println("File Replication: " + status.getReplication());
    //         System.out.println("File Block Size: " + status.getBlockSize() + " bytes");
            
    //         // Obtenir les informations sur les blocs
    //         System.out.println("\n=== Informations sur les Blocs ===");
    //         BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            
    //         for (BlockLocation blockLocation : blockLocations) {
    //             String[] hosts = blockLocation.getHosts();
    //             System.out.println("Block offset: " + blockLocation.getOffset());
    //             System.out.println("Block length: " + blockLocation.getLength());
    //             System.out.print("Block hosts: ");
    //             for (String host : hosts) {
    //                 System.out.print(host + " ");
    //             }
    //             System.out.println();
    //         }
            
    //         // Renommer le fichier
    //         System.out.println("\n=== Renommage du fichier ===");
    //         Path newFilepath = new Path("/user/root/input", "achats.txt");
    //         boolean renamed = fs.rename(filepath, newFilepath);
    //         if (renamed) {
    //             System.out.println("File renamed successfully to: achats.txt");
    //         } else {
    //             System.out.println("File rename failed");
    //         }
            
    //         fs.close();
            
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }

        public static void main(String[] args) {
        // Vérifier les arguments
        if (args.length < 3) {
            System.out.println("Usage: hadoop jar HadoopFileStatus.jar <chemin> <nom_fichier> <nouveau_nom>");
            System.exit(1);
        }
        
        String cheminDossier = args[0];
        String nomFichier = args[1];
        String nouveauNom = args[2];
        
        Configuration conf = new Configuration();
        FileSystem fs;
        
        try {
            fs = FileSystem.get(conf);
            
            // Créer le chemin complet
            Path filepath = new Path(cheminDossier, nomFichier);
            
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }
            
            FileStatus status = fs.getFileStatus(filepath);
            
            // Afficher les informations
            System.out.println("=== Informations du Fichier ===");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Group: " + status.getGroup());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize() + " bytes");
            
            // Blocs
            System.out.println("\n=== Informations sur les Blocs ===");
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            
            // Renommer
            System.out.println("\n=== Renommage du fichier ===");
            Path newFilepath = new Path(cheminDossier, nouveauNom);
            boolean renamed = fs.rename(filepath, newFilepath);
            if (renamed) {
                System.out.println("File renamed successfully to: " + nouveauNom);
            } else {
                System.out.println("File rename failed");
            }
            
            fs.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}