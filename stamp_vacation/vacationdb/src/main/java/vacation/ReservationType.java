package vacation;

public enum ReservationType {
    car(0), flight(1), room(2);

    private int type;
    private ReservationType(int type) { this.type = type; }

    public int getType() { return type; }

    private static ReservationType[] types = new ReservationType[]{car, flight, room};
    public static ReservationType forType(int type) {return types[type];}
}
