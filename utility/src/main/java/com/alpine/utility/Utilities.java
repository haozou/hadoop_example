package com.alpine.utility;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hao on 7/2/15.
 */
final public class Utilities {
    public static String findJarAbsPath(Class clazz) throws URISyntaxException {
        File f = new File(clazz.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        return "file://" + f.getAbsolutePath();
    }

    public static List<String> findJarAbsPaths(List<Class> classes) throws URISyntaxException {
        List<String> paths = new ArrayList<String>();
        for (Class clazz : classes) {
            paths.add(findJarAbsPath(clazz));
        }
        return paths;
    }

    public static String libjars(List<Class> classes) throws URISyntaxException {
        StringBuilder builder = new StringBuilder();
        for (String jar : findJarAbsPaths(classes)) {
            builder.append(jar).append(",");
        }
        return builder.toString().substring(0, builder.toString().length() - 1);
    }
}
