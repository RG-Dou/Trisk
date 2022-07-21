package linearRoad.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExpenditurePool implements Serializable {
    public List<ExpenditureData> lists = new ArrayList<>();

    public ExpenditurePool(){
    }

    public void addData(ExpenditureData data){
        lists.add(data);
    }
}
