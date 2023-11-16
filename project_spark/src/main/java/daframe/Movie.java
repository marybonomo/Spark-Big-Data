package daframe;

import java.io.Serializable;
public class Movie implements Serializable {
private String name;
private Double rating;
private String cinema;
public Movie(String name, Double rating, String cinema) {
super();
this.name = name;
this.rating = rating;
this.cinema = cinema;
}
public Movie()
{
}
public String getName() {
return name;
}
public void setName(String name) {
this.name = name;
}
public Double getRating() {
return rating;
}
public void setRating(Double rating) {
this.rating = rating;
}
public String getCinema() {
return cinema;
}
public void setCinema(String cinema) {
this.cinema = cinema;
}
}