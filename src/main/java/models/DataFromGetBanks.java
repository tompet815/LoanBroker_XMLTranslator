package models;


import java.io.Serializable;
import java.util.Date;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
@XmlRootElement(name="Data") 
@XmlAccessorType(XmlAccessType.FIELD)
public class DataFromGetBanks  implements Serializable {

    @XmlElementWrapper(name = "bankExchangeNames")
    @XmlElement(name = "bankExchangeName")
    private List<Bank> bankExchangeNames;
    private String ssn;
    private int creditScore;
    private double loanAmoount;
    private int loanDuration;
    public DataFromGetBanks() {
    }

    public DataFromGetBanks(List<Bank> bankExchangeNames, String ssn, int creditScore, double loanAmoount, int loanDuration) {
        this.bankExchangeNames = bankExchangeNames;
        this.ssn = ssn;
        this.creditScore = creditScore;
        this.loanAmoount = loanAmoount;
        this.loanDuration = loanDuration;
    }

    public List<Bank> getBankExchangeNames() {
        return bankExchangeNames;
    }

    public void setBankExchangeNames(List<Bank> bankExchangeNames) {
        this.bankExchangeNames = bankExchangeNames;
    }

    public String getSsn() {
        return ssn;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public int getCreditScore() {
        return creditScore;
    }

    public void setCreditScore(int creditScore) {
        this.creditScore = creditScore;
    }

    public double getLoanAmoount() {
        return loanAmoount;
    }

    public void setLoanAmoount(double loanAmoount) {
        this.loanAmoount = loanAmoount;
    }

    public int getLoanDuration() {
        return loanDuration;
    }

    public void setLoanDuration(int loanDuration) {
        this.loanDuration = loanDuration;
    }

    
   
   
}
