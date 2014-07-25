package utils;


public class LatLon<T> {
    
	private T lat;
    private T lon;
    
    public LatLon(T lat, T lon){
        this.lat = lat;
        this.lon = lon;
    }
    
    public T getLat(){ return lat; }
    
    public T getLon(){ return lon; }
    
    public void setLat(T lat){ this.lat = lat; }
    
    public void setLon(T lon){ this.lon = lon; }

}
