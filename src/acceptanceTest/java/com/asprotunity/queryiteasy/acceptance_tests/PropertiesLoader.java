package com.asprotunity.queryiteasy.acceptance_tests;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

public abstract class PropertiesLoader {
    public static Properties loadProperties(Path path) throws IOException {
        Properties result = new Properties();
        try (InputStream is = new FileInputStream(path.toFile())) {
            result.load(is);
            return result;
        }
    }
}
