package io.confluent.connect.jdbc.source;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

public class Test {

  public static class Test1 {
    public final String s1;
    public final String s2;

    public Test1(List<String> l) {
      s1 = l.get(0);
      s2 = l.get(1);
    }
    @Override
    public String toString() {
      return "Test1(" +
              "s1='" + s1 + '\'' +
              ", s2='" + s2 + '\'' +
              ')';
    }
  }

  public static class Test2 {
    public final String s1;
    public final String s2;
    public final String s3;

    public Test2(List<String> l) {
      s1 = l.get(0);
      s2 = l.get(1);
      s3 = l.get(2);
    }

    @Override
    public String toString() {
      return "Test2(" +
              "s1='" + s1 + '\'' +
              ", s2='" + s2 + '\'' +
              ", s3='" + s3 + '\'' +
              ')';
    }
  }
  public static void main(String[] args) {
    List<String> l = Arrays.asList(new String[] {"one", "two", "three"});
    Class<?> cl = null;
    try {
      cl = Class.forName("io.confluent.connect.jdbc.source.Test$Test1");
      Constructor<?> cons = cl.getConstructor(List.class);
      Object o = cons.newInstance(l);
      System.out.println(o.toString());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }

    System.out.println(new Test1(l));
    System.out.println(new Test2(l));
  }

}
