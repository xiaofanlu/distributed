package util;

public interface KVStore {
  public boolean add(String song, String url);

  public boolean delete(String song, String url);

  public boolean edit(String song, String url);
}
