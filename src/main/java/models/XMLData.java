package models;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class XMLData implements Serializable {

    private String ssn;
    private int creditScore;
    private double loanAmount;
    private String loanDuration;

    public XMLData(String ssn, int creditScore, double loanAmount, String loanDuration) {
        this.ssn = ssn;
        this.creditScore = creditScore;
        this.loanAmount = loanAmount;
        this.loanDuration = loanDuration;
    }

    public String getSsn() {
        return ssn;
    }

    public int getCreditScore() {
        return creditScore;
    }

    public double getLoanAmount() {
        return loanAmount;
    }

    public String getLoanDuration() {
        return loanDuration;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public void setCreditScore(int creditScore) {
        this.creditScore = creditScore;
    }

    public void setLoanAmount(double loanAmount) {
        this.loanAmount = loanAmount;
    }

    public void setLoanDuration(String loanDuration) {
        this.loanDuration = loanDuration;
    }

    @Override
    public String toString() {
        return "Data{" + "ssn=" + ssn + ", creditScore=" + creditScore + ", loanAmount=" + loanAmount + ", loanDuration=" + loanDuration + '}';
    }

}
