/*
  Copyright 2016 Jan Buchholz, Stephan Kemper

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package randomWalkWithRestart.Utils;

import static org.junit.Assert.assertEquals;
import java.math.BigDecimal;
import java.util.LinkedList;
import org.junit.Test;
import randomWalkWithRestart.Functions.MessageType;

public class MathUtilsTest {
    /**
     * Test method for {@link randomWalkWithRestart.Utils.MathUtils#sum(java.util.Iterator)}.
     */
    @Test
    public void testSum() {
        LinkedList<MessageType<Double>> liste = new LinkedList<>();
        MessageType<Double> message = new MessageType<>(0.1d);
        liste.add(message);
        liste.add(message);
        liste.add(message);
        BigDecimal sum;
        sum = MathUtils.sum(liste.iterator());
        System.out.println(sum);
        assertEquals(BigDecimal.valueOf(0.3d), sum);
    }
}
