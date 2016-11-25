package com.enedis;

public class RegionDataLocality {
    public String regionName;
    public Float dataLocality;

    public RegionDataLocality(String regionName, Float dataLocality) {
        this.regionName = regionName;
        this.dataLocality = dataLocality;
    }

    public Float getDataLocality() {
        return dataLocality;
    }
}
