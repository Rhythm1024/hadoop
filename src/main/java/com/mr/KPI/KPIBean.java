package com.mr.KPI;

/**
 * Created by Administrator on 2018/01/24.
 */
public class KPIBean {

    private String userIp;
    private String requestTime;
    private String requestPage;
    private String fromPage;
    private String theOther;
    private String requestStatus;

    public static KPIBean parse(String line) {
        String[] arr = line.split(" ");
        KPIBean kpi = new KPIBean();
            kpi.setUserIp(arr[0]);
            kpi.setRequestTime(arr[3].substring(1));
            kpi.setRequestPage(arr[6]);
            kpi.setRequestStatus(arr[8]);
            kpi.setFromPage(arr[10]);
            kpi.setTheOther(arr[11].substring(1,arr[11].length()));
            return kpi;
    }


    @Override
    public String toString() {
        return userIp + '\t'
                 + requestTime + '\t'+requestPage+'\t'  + fromPage + '\t'
                + theOther + '\t';
    }


    public String getUserIp() {
        return userIp;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    public String getRequestStatus() {
        return requestStatus;
    }

    public void setRequestStatus(String requestStatus) {
        this.requestStatus = requestStatus;
    }

    public String getRequestPage() {
        return requestPage;
    }

    public void setRequestPage(String requestPage) {
        this.requestPage = requestPage;
    }


    public String getFromPage() {
        return fromPage;
    }

    public void setFromPage(String fromPage) {
        this.fromPage = fromPage;
    }

    public String getTheOther() {
        return theOther;
    }

    public void setTheOther(String theOther) {
        this.theOther = theOther;
    }

    public static void main(String[] args) {
        String s ="65.52.104.84 - - [04/Jan/2012:00:00:07 +0800] \"GET /viewthread.php?tid=1072164 HTTP/1.1\" 301 5 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"";
        KPIBean kpi = KPIBean.parse(s);
        System.out.println(kpi.toString());
    }
}