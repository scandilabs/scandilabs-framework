package org.catamarancode.util;

public final class ZipCodeUtils {
	
	private ZipCodeUtils() {}
	
	/**
     * Note for longitude this is a crude approximation (since the distance
     * between longitudes becomes shorter when you move away from the equator)
     * but since this service filters the results anyway it should work ok.
     * 
     * @see http://en.wikipedia.org/wiki/Longitude#Degree_length
     */
    public static final double MILES_PER_ARC_DEGREE = 69;
    
    public static final double RADIUS_OF_EARTH = 3963.0;
    
    public static final double DECIMAL_TO_RADIANS_CONSTANT = 57.2958;
	
	public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
		double distance = RADIUS_OF_EARTH * Math.acos(Math.sin(lat1/DECIMAL_TO_RADIANS_CONSTANT) * Math.sin(lat2/DECIMAL_TO_RADIANS_CONSTANT) + Math.cos(lat1/DECIMAL_TO_RADIANS_CONSTANT) * Math.cos(lat2/DECIMAL_TO_RADIANS_CONSTANT) * Math.cos(lon2/DECIMAL_TO_RADIANS_CONSTANT - lon1/DECIMAL_TO_RADIANS_CONSTANT));
		if (Double.isNaN(distance)) {
		    return 0;
		}
		//System.out.println("Distance: " + distance);
		//System.out.println("NaN: " + Double.NaN);
		return distance;
	}

}
