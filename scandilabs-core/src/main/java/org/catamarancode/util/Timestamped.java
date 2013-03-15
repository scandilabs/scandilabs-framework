package org.catamarancode.util;

import java.util.Date;

public interface Timestamped {

    void setCreatedTime(Date created);

    Date getCreatedTime();

    void setLastModifiedTime(Date modified);

    Date getLastModifiedTime();
}
