package utils;

import storm.trident.state.ValueUpdater;

public class LatLon<Lat, Lon> {
    
	private Lat lat;
    private Lon lon;
    
    public LatLon(Lat lat, Lon lon){
        this.lat = lat;
        this.lon = lon;
    }
    
    public Lat getLat(){ return lat; }
    
    public Lon getLon(){ return lon; }
    
    public void setLat(Lat lat){ this.lat = lat; }
    
    public void setLon(Lon lon){ this.lon = lon; }

}
