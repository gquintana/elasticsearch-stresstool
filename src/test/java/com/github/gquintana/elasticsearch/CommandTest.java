package com.github.gquintana.elasticsearch;

import com.beust.jcommander.JCommander;
import org.elasticsearch.common.io.Streams;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CommandTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Command command = new Command() {
        @Override
        protected Task createTask() {
            return null;
        }
    };

    @Test
    public void testLoadPassword_File() throws IOException {
        // Given
        String password = "Pa$5wörd";
        File passwordFile = temporaryFolder.newFile("password.txt");
        try(FileWriter fileWriter = new FileWriter(passwordFile)) {
            Streams.copy(password, fileWriter);
        }
        command.setUserName("username");
        command.setPassword(("@"+passwordFile.getAbsolutePath()).toCharArray());
        // When
        command.loadPassword();
        // Then
        assertEquals(password, new String(command.getPassword()));
    }

    @Test @Ignore
    public void testLoadPassword_Console() throws IOException {
        // Given
        command.setUserName("username");
        command.setPassword("prompt".toCharArray());
        // When
        System.out.println("Type 'password'");
        command.loadPassword();
        // Then
        assertEquals("password", command.getPassword());
    }

    @Test
    public void testJCommander() {
        // Given
        JCommander commander = new JCommander(command);
        // When
        String password = "ZeP@zzw0rd";
        commander.parse("-u","username","-p", password);
        // Then
        assertEquals("username", command.getUserName());
        assertEquals(password, new String(command.getPassword()));
    }

}