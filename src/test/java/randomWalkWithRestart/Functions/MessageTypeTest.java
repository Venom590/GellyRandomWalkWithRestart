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
package randomWalkWithRestart.Functions;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Test class for @link {MessageType}
 */
public class MessageTypeTest {

    /**
     * Test method for
     * {@link randomWalkWithRestart.Functions.MessageType#getValue()}.
     */
    @Test
    public void testGetValueDouble() {
        MessageType<Double> mt = new MessageType<Double>(512.128d);
        Double value = mt.getValue();

        assertEquals(Double.valueOf(512.128d), value);

    }

    /**
     * Test method for
     * {@link randomWalkWithRestart.Functions.MessageType#setValue(java.lang.Object)}.
     */
    @Test
    public void testSetValue() {
        MessageType<Double> mt = new MessageType<Double>();
        mt.setValue(512.128);

        assertEquals(Double.valueOf(512.128), mt.getValue());
    }

    /**
     * Test method for
     * {@link randomWalkWithRestart.Functions.MessageType#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
        MessageType<Double> mt = new MessageType<>();
        mt.setValue(512.128);

        MessageType<Double> mt2 = new MessageType<>();
        mt2.setValue(512.128);

        assertEquals(mt, mt2);
    }

}
